package tasks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"io"
	"net/http"
	"regexp"
	"scheduler/logger"
	"strings"
	"time"
)

var (
	filePathTimeRe    = regexp.MustCompile(`\d{12}`)
	defaultHttpClient = &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100, // Increased for potentially many concurrent operations
			IdleConnTimeout:     90 * time.Second,
			MaxIdleConnsPerHost: 20, // Adjust based on how many concurrent requests hit the same CH instance
			MaxConnsPerHost:     0,  // No limit on total connections per host (managed by MaxIdleConnsPerHost for idle)
		},
	}
)

// convertMapSliceToStructSlice uses mapstructure for more efficient and flexible conversion.
func convertMapSliceToStructSlice[T any](mapSlice []map[string]interface{}, logger *logger.Logger) ([]T, error) {
	if mapSlice == nil {
		return nil, nil
	}

	structSlice := make([]T, 0, len(mapSlice))
	for i, itemMap := range mapSlice {
		var targetStruct T
		// Custom decoder hook for time.Time if ClickHouse returns time as string in a specific format
		// that mapstructure's default doesn't handle for a time.Time field.
		// However, ClickHouse JSON format usually gives numbers for timestamps or ISO 8601 strings.
		// If your target struct field is string for time, it's simpler. If it's time.Time, ensure compatibility.
		config := &mapstructure.DecoderConfig{
			Result:           &targetStruct,
			TagName:          "json", // Assumes your struct fields use "json" tags matching map keys
			WeaklyTypedInput: true,   // Allows for some type coercion (e.g., int to float)
			// Add custom hooks if needed, e.g., string to time.Time
			// DecodeHook: mapstructure.ComposeDecodeHookFunc(
			//     mapstructure.StringToTimeHookFunc(time.RFC3339), // Example
			// ),
		}
		decoder, err := mapstructure.NewDecoder(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create mapstructure decoder for item at index %d: %w", i, err)
		}

		if err := decoder.Decode(itemMap); err != nil {
			logger.Debugf("Problematic map item at index %d: %+v", i, itemMap)
			return nil, fmt.Errorf("failed to decode map to %T for item at index %d: %w", targetStruct, i, err)
		}
		structSlice = append(structSlice, targetStruct)
	}
	return structSlice, nil
}

// doCHQuery executes a generic ClickHouse query (typically SELECT).
func doCHQuery(sqlQuery string, chdburl string, logger *logger.Logger) ([]byte, error) {
	t1 := time.Now()
	// Base URL params for SELECT queries returning JSON
	// Make sure your chdburl doesn't already contain '?'
	url := fmt.Sprintf("%s?add_http_cors_header=1&default_format=JSON&max_result_rows=10000&max_result_bytes=100000000&result_overflow_mode=break", chdburl)

	req, err := http.NewRequest("POST", url, strings.NewReader(sqlQuery))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	// Add other headers if necessary, e.g., X-ClickHouse-User, X-ClickHouse-Key for auth

	resp, err := defaultHttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request to ClickHouse: %w", err)
	}
	defer resp.Body.Close()

	logger.Debugf("ClickHouse query execution time: %s for query: %s", time.Since(t1), sqlQuery)

	bodyBytes, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("clickhouse request status %s, error reading body: %w", resp.Status, readErr)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse request failed with status %s, body: %s", resp.Status, string(bodyBytes))
	}

	return bodyBytes, nil
}

// doCHInsert sends data to ClickHouse using JSONEachRow format for INSERT statements.
func doCHInsert(dataItem interface{}, tableName string, chdburl string, logger *logger.Logger) error {
	jsonData, err := json.Marshal(dataItem) // Marshals a single struct/map to a JSON object
	if err != nil {
		return fmt.Errorf("failed to marshal data for insert into %s: %w", tableName, err)
	}

	// The query itself, data goes into the POST body
	// Make sure your chdburl doesn't already contain '?'
	query := fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", tableName)
	fullURL := fmt.Sprintf("%s?query=%s", chdburl, query) // URL encode query? ClickHouse usually handles it.
	// query_param in URL is standard.

	req, err := http.NewRequest("POST", fullURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating insert request for %s: %w", tableName, err)
	}
	// Content-Type for JSONEachRow can be application/json or text/plain.
	// Since we send one JSON object per line (here, just one object), this is fine.
	req.Header.Set("Content-Type", "application/json")

	resp, err := defaultHttpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making insert request to %s: %w", tableName, err)
	}
	defer resp.Body.Close()

	respBodyBytes, readErr := io.ReadAll(resp.Body) // Read body for potential error messages or info

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		logger.Debugf("Successfully inserted data into %s. Response (if any): %s", tableName, string(respBodyBytes))
		return nil
	}

	errMsg := fmt.Sprintf("failed to insert data into %s, status: %s", tableName, resp.Status)
	if readErr == nil && len(respBodyBytes) > 0 {
		errMsg = fmt.Sprintf("%s, response: %s", errMsg, string(respBodyBytes))
	} else if readErr != nil {
		errMsg = fmt.Sprintf("%s, error reading response body: %v", errMsg, readErr)
	}
	return fmt.Errorf(errMsg)
}

// fetchAndPrepareReportData is a helper to fetch, convert, and prepare the first report item.
func fetchAndPrepareReportData[T any](
	filePath, deviceName, chdburl string,
	logger *logger.Logger,
	buildSelectSQL func(filePath string) string,
	reportSetter func(report *T, deviceName string, reportTime time.Time),
) (*T, error) {
	calculateSql := buildSelectSQL(filePath)
	logger.Debugf("Executing SQL for %s: %s", deviceName, calculateSql)
	fmt.Println(calculateSql)
	jsonDataBytes, err := doCHQuery(calculateSql, chdburl, logger)
	if err != nil {
		return nil, fmt.Errorf("query execution failed for %s: %w", deviceName, err)
	}
	// logger.Debugf("Raw JSON response for %s: %s", deviceName, string(jsonDataBytes))

	var response CHDBJsonStruct
	if err := json.Unmarshal(jsonDataBytes, &response); err != nil {
		return nil, fmt.Errorf("error unmarshaling chdb JSON response for %s: %w. JSON: %s", deviceName, err, string(jsonDataBytes))
	}

	if len(response.Data) == 0 {
		logger.Infof("No data returned from query for file: %s, device: %s", filePath, deviceName)
		return nil, nil // Not an error, but no data to process
	}

	reports, err := convertMapSliceToStructSlice[T](response.Data, logger)
	if err != nil {
		return nil, fmt.Errorf("error converting map slice for %s: %w", deviceName, err)
	}
	if len(reports) == 0 {
		logger.Warnf("Converted slice is empty, though response.Data was not, for file: %s, device: %s", filePath, deviceName)
		return nil, nil // No data after conversion
	}

	reportItem := &reports[0] // Get a pointer to the first item to modify it

	matches := filePathTimeRe.FindStringSubmatch(filePath)
	var reportTime time.Time
	if len(matches) > 0 {
		datetimeStr := matches[0]
		layout := "200601021504" // YYYYMMDDHHMM
		parsedTime, pErr := time.Parse(layout, datetimeStr)
		if pErr != nil {
			return nil, fmt.Errorf("failed to parse datetime '%s' from filepath '%s' for device '%s': %w", datetimeStr, filePath, deviceName, pErr)
		}
		reportTime = parsedTime.Truncate(10 * time.Minute)
	} else {
		return nil, fmt.Errorf("could not extract 12-digit timestamp from filepath '%s' for device '%s'", filePath, deviceName)
	}

	reportSetter(reportItem, deviceName, reportTime)

	return reportItem, nil
}

func CalculateSummaryData(filePath, deviceName string, logger *logger.Logger, chdburl string) error {
	buildSQL := func(fp string) string {
		// Note: Ensure filePath is properly escaped if it could contain special characters,
		// though file paths are usually less problematic than other user inputs.
		// ClickHouse's file function should handle typical paths.
		return fmt.Sprintf("SELECT\n    COUNT(MC082) AS Count, \n    SUM(IsEffectWind) AS EffectWindHours,\n    AVG(MC004) AS Met1sWSpdAvg,\n    MAX(MC004) AS Met1sWSpdMax,\n    MIN(MC004) AS Met1sWSpdMin,\n    AVG(MC082) AS Met30sWSpdAvg,\n    MAX(MC082) AS Met30sWSpdMax,\n    MIN(MC082) AS Met30sWSpdMin,\n    AVG(MC083) AS Met600sWSpdAvg,\n    MAX(MC083) AS Met600sWSpdMax,\n    MIN(MC083) AS Met600sWSpdMin,\n    AVG(MC093) AS MetAirDensityAvg,\n\n    (MAX(MC006) - MIN(MC006)) / ANY_VALUE(RatedPower) * 3600 AS EquivalentHours, \n    ANY_VALUE(RatedPower) AS RatedPowerData,\n    MIN(MC006) AS PreTotalGenerationFirst,\n    MAX(MC006) AS GriActiveEnergyDelLast,\n    (MAX(MC006) - MIN(MC006)) AS GriActiveEnergyDelTotal,\n    SUM(TheoreticalGeneration) AS TheoreticalGeneration,\n    MIN(MC062) AS PreTotalEleConsumptionFirst,\n    MAX(MC062) AS GriActiveEnergyRcvLast,\n    (MAX(MC062) - MIN(MC062)) AS GriActiveEnergyRcvSection,\n    AVG(MC061) AS GriActivePowerTotalAvg,\n    MAX(MC061) AS GriActivePowerTotalMax,\n    MIN(MC061) AS GriActivePowerTotalMin,\n    AVG(MD002) AS GriReactivePowerTotalAvg,\n    MAX(MD002) AS GriReactivePowerTotalMax,\n    MIN(MD002) AS GriReactivePowerTotalMin,\n    AVG(MC085) AS CnvGenPower30sAvg,\n    MAX(MC085) AS CnvGenPower30sMax,\n    MIN(MC085) AS CnvGenPower30sMin,\n    AVG(MC086) AS CnvGenPower600sAvg,\n    MAX(MC086) AS CnvGenPower600sMax,\n    MIN(MC086) AS CnvGenPower600sMin,\n    \n    COUNT(CASE WHEN MD001 >= RatedPower THEN 1 END) AS FullLoadDurationDay, \n    SUM(Generation) / 10000.0 AS GriActiveEnergyDelUnit,\n    SUM(Consumption) / 10000.0 AS GriActiveEnergyRcvUnit,\n\n\n    AVG(MC015) AS MetTmpAvg,\n    MAX(MC015) AS MetTmpMax,\n    MIN(MC015) AS MetTmpMin,\n    AVG(MC014) AS NacTmpAvg,\n    MAX(MC014) AS NacTmpMax,\n    MIN(MC014) AS NacTmpMin,\n    AVG(MC091) AS TowCbtTmpAvg,\n    MAX(MC091) AS TowCbtTmpMax,\n    MIN(MC091) AS TowCbtTmpMin,\n    SUM(CASE WHEN MA022 = 1 THEN 1 ELSE 0 END) AS YawAcwHours,\n    SUM(CASE WHEN YawCcwTimes = 1 THEN 1 ELSE 0 END) AS YawAcwTimes, \n    SUM(CASE WHEN MA021 = 1 THEN 1 ELSE 0 END) AS YawCwHours,\n    SUM(YawCwTimes) AS YawCwTimesSum, \n    (SUM(CASE WHEN MA022 = 1 THEN 1 ELSE 0 END) + SUM(CASE WHEN MA021 = 1 THEN 1 ELSE 0 END)) AS YawHoursTotal,\n    SUM(CASE WHEN MA2353 = 1 THEN 1 ELSE 0 END) AS GenReactQLimitStateDuration,\n    SUM(GenReactQLimitStateTimes) AS GenReactQLimitStateTimesSum\nFROM\n    file('%s', Parquet);", fp)
	}

	setter := func(report *SummaryData, devName string, rTime time.Time) {
		report.DeviceName = devName
		report.Time = rTime
	}

	report, err := fetchAndPrepareReportData[SummaryData](filePath, deviceName, chdburl, logger, buildSQL, setter)
	if err != nil {
		return fmt.Errorf("failed to fetch and prepare summary data for %s: %w", deviceName, err)
	}
	if report == nil {
		logger.Infof("No summary data to insert for file %s, device %s", filePath, deviceName)
		return nil
	}

	// Prepare payload for JSONEachRow, especially formatting time.Time
	// The SummaryData struct should have `json` tags for all fields matching ClickHouse column names.
	// Time field needs special handling for JSON marshaling if it's time.Time.
	// Best to create a map or a dedicated "forInsert" struct.
	//payload := summaryDataToInsertPayload(*report)
	//
	//if err := doCHInsert(payload, "db_report.SummaryReport", chdburl, logger); err != nil {
	//	return fmt.Errorf("error inserting summary data for %s: %w", deviceName, err)
	//}
	if err := insertSummaryData(*report, chdburl, logger); err != nil {
		return fmt.Errorf("error inserting summary data for %s: %w", deviceName, err)
	}

	logger.Debugf("Successfully inserted summary report for %s", deviceName)
	return nil
}

func CalculateTurbineAvailabilityMetrics(filePath, deviceName string, logger *logger.Logger, chdburl string) error {
	buildSQL := func(fp string) string {
		return fmt.Sprintf("WITH AggregatedData AS (\n    SELECT\n        SUM(CASE WHEN MC143 = 40 THEN 1 ELSE 0 END) AS FaultShutdownDuration,\n        SUM(CASE WHEN MC143 = 50 THEN 1 ELSE 0 END) AS OverhaulDuration,\n        SUM(CASE WHEN MC143 = 60 THEN 1 ELSE 0 END) AS MaintenanceDuration, \n        SUM(CASE WHEN MC143 = 70 THEN 1 ELSE 0 END) AS GridShutdownDuration,\n        SUM(CASE WHEN MC143 = 81 THEN 1 ELSE 0 END) AS LocalShutdownDuration,\n        SUM(CASE WHEN MC143 = 80 THEN 1 ELSE 0 END) AS RemoteShutdownDuration,\n        SUM(CASE WHEN MC143 = 90 THEN 1 ELSE 0 END) AS WeatherShutdownDuration,\n        SUM(CASE WHEN MC143 = 110 THEN 1 ELSE 0 END) AS LimitPowerDuration,\n        SUM(CASE WHEN MC143 = 111 THEN 1 ELSE 0 END) AS LimitShutdownDuration,\n        SUM(CASE WHEN MC143 = 100 THEN 1 ELSE 0 END) AS StandbyDuration,\n        SUM(CASE WHEN MC143 = 120 THEN 1 ELSE 0 END) AS NormalGenerationDuration,\n        SUM(FaultShutdownTimes) AS FaultShutdownTimes,\n        SUM(OverhaulTimes) AS OverhaulTimes,\n        SUM(MaintenanceTimes) AS MaintenanceTimes,\n        SUM(GridShutdownTimes) AS GridShutdownTimes,\n        SUM(LocalShutdownTimes) AS LocalShutdownTimes,\n        SUM(RemoteShutdownTimes) AS RemoteShutdownTimes,\n        SUM(WeatherShutdownTimes) AS WeatherShutdownTimes,\n        SUM(LimitPowerTimes) AS LimitPowerTimes,\n        SUM(LimitShutdownTimes) AS LimitShutdownTimes,\n        SUM(StandbyTimes) AS StandbyTimes,\n        SUM(NormalGenerationTimes) AS NormalGenerationTimes,\n        SUM(InterruptionTimes) AS InterruptionTimes,\n        COUNT(*) AS Count\n    FROM file('%s', Parquet)\n)\nSELECT\n    FaultShutdownDuration,\n    OverhaulDuration,\n    MaintenanceDuration,\n    GridShutdownDuration,\n    LocalShutdownDuration,\n    RemoteShutdownDuration,\n    WeatherShutdownDuration,\n    LimitPowerDuration,\n    LimitShutdownDuration,\n    StandbyDuration,\n    NormalGenerationDuration,\n    FaultShutdownTimes,\n    OverhaulTimes,\n    MaintenanceTimes,\n    GridShutdownTimes,\n    LocalShutdownTimes,\n    RemoteShutdownTimes,\n    WeatherShutdownTimes,\n    LimitPowerTimes,\n    LimitShutdownTimes,\n    StandbyTimes,\n    NormalGenerationTimes,\n    InterruptionTimes,\n    Count,\n    (MaintenanceDuration+GridShutdownDuration +LocalShutdownDuration +RemoteShutdownDuration +WeatherShutdownDuration +LimitPowerDuration +LimitShutdownDuration +StandbyDuration +NormalGenerationDuration) as Availabletime,\n    (FaultShutdownDuration + OverhaulDuration) as UnAvailabletime,\n    (1.0 - (FaultShutdownDuration + OverhaulDuration) * 1.0 / NULLIF(Count, 0)) AS Availability,\n    (600 - Count) AS InterruptionDuration_Calculated, \n    ((Count - FaultShutdownDuration) * 1.0 / NULLIF(FaultShutdownTimes, 0)) AS MTBFDuration,\n    (FaultShutdownDuration * 1.0 / NULLIF(FaultShutdownTimes, 0)) AS MTTRDuration\nFROM\n    AggregatedData;", fp)
	}
	setter := func(report *TurbineAvailabilityMetrics, devName string, rTime time.Time) {
		report.DeviceName = devName
		report.Time = rTime
	}

	report, err := fetchAndPrepareReportData[TurbineAvailabilityMetrics](filePath, deviceName, chdburl, logger, buildSQL, setter)
	if err != nil {
		return fmt.Errorf("failed to fetch/prepare turbine availability for %s: %w", deviceName, err)
	}
	if report == nil {
		logger.Infof("No turbine availability data to insert for file %s, device %s", filePath, deviceName)
		return nil
	}

	//payload := turbineAvailabilityToInsertPayload(*report)
	//if err := doCHInsert(payload, "db_report.TurbineAvailabilityMetrics", chdburl, logger); err != nil {
	//	return fmt.Errorf("error inserting turbine availability for %s: %w", deviceName, err)
	//}

	if err := insertTurbineAvailabilityMetrics(*report, chdburl, logger); err != nil {
		return fmt.Errorf("error inserting turbine availability report for %s: %w", deviceName, err)
	}

	logger.Debugf("Successfully inserted turbine availability report for %s", deviceName)

	//logger.Debugf("Successfully inserted turbine availability report for %s", deviceName)
	return nil
}

func CalculateEfficiencyMetrics(filePath, deviceName string, logger *logger.Logger, chdburl string) error {
	buildSQL := func(fp string) string {
		return fmt.Sprintf("WITH SourceData AS (\n    SELECT\n        MC006,\n        preTotalGeneration,\n        FaultLossGeneration,\n        OverhaulLossGeneration,\n        MaintainLossGeneration,\n        GridLossGeneration,\n        RemoteLossGeneration,\n        LocalLossGeneration,\n        WeatherLossGeneration,\n        LimitLossGeneration,\n        TheoreticalGeneration,\n        ROW_NUMBER() OVER (ORDER BY time DESC) AS rn_desc,\n        ROW_NUMBER() OVER (ORDER BY time ASC) AS rn_asc\n    FROM file('%s', Parquet)\n),\nAggregatedValues AS (\n    SELECT\n        count(MC006) AS Count,\n        MAX(CASE WHEN rn_desc = 1 THEN MC006 END) AS latest_mc006,\n        MAX(CASE WHEN rn_asc = 1 THEN preTotalGeneration END) AS earliest_preTotalGeneration,\n        SUM(FaultLossGeneration) AS sum_fault_loss,\n        SUM(OverhaulLossGeneration) AS sum_overhaul_loss,\n        SUM(MaintainLossGeneration) AS sum_maintain_loss,\n        SUM(GridLossGeneration) AS sum_grid_loss,\n        SUM(RemoteLossGeneration) AS sum_remote_loss,\n        SUM(LocalLossGeneration) AS sum_local_loss,\n        SUM(WeatherLossGeneration) AS sum_weather_loss,\n        SUM(LimitLossGeneration) AS sum_limit_loss,\n        SUM(TheoreticalGeneration) AS sum_theoretical_gen\n    FROM SourceData\n),\nCalculatedMetrics AS (\n    SELECT\n        Count,\n        latest_mc006,\n        earliest_preTotalGeneration,\n        sum_theoretical_gen,\n        (latest_mc006 - earliest_preTotalGeneration) AS actual_generation_delta,\n        (sum_fault_loss + sum_overhaul_loss + sum_maintain_loss + \n         sum_grid_loss + sum_remote_loss + sum_local_loss + \n         sum_weather_loss) / 3600.0 AS total_loss_div_3600,\n        sum_fault_loss / 3600.0 / 10000.0 AS FaultLossGeneration,\n        sum_overhaul_loss / 3600.0 / 10000.0 AS OverhaulLossGeneration,\n        sum_maintain_loss / 3600.0 / 10000.0 AS MaintainLossGeneration,\n        sum_grid_loss / 3600.0 / 10000.0 AS GridLossGeneration,\n        sum_local_loss / 3600.0 / 10000.0 AS LocalLossGeneration,\n        sum_remote_loss / 3600.0 / 10000.0 AS RemoteLossGeneration,\n        sum_weather_loss / 3600.0 / 10000.0 AS WeatherLossGeneration,\n        sum_limit_loss / 3600.0 / 10000.0 AS LimitLossGeneration\n    FROM AggregatedValues\n)\nSELECT\n    Count,\n    (1.0 - (actual_generation_delta / \n            NULLIF(actual_generation_delta + total_loss_div_3600, 0))) * 100.0 AS Discrepancy,\n    (actual_generation_delta / NULLIF(sum_theoretical_gen, 0)) * 3600.0 * 100.0 AS EnergyAvailability,\n    FaultLossGeneration,\n    OverhaulLossGeneration,\n    MaintainLossGeneration,\n    GridLossGeneration,\n    LocalLossGeneration,\n    RemoteLossGeneration,\n    WeatherLossGeneration,\n    LimitLossGeneration\nFROM CalculatedMetrics;", fp)
	}
	setter := func(report *EfficiencyMetrics, devName string, rTime time.Time) {
		report.DeviceName = devName
		report.Time = rTime
	}

	report, err := fetchAndPrepareReportData[EfficiencyMetrics](filePath, deviceName, chdburl, logger, buildSQL, setter)
	if err != nil {
		return fmt.Errorf("failed to fetch/prepare efficiency metrics for %s: %w", deviceName, err)
	}
	if report == nil {
		logger.Infof("No efficiency metrics data to insert for file %s, device %s", filePath, deviceName)
		return nil
	}

	//payload := efficiencyMetricsToInsertPayload(*report)
	//if err := doCHInsert(payload, "db_report.EfficiencyMetrics", chdburl, logger); err != nil {
	//	return fmt.Errorf("error inserting efficiency metrics for %s: %w", deviceName, err)
	//}

	if err := insertEfficiencyMetrics(*report, chdburl, logger); err != nil {
		return fmt.Errorf("error inserting efficiency metrics report for %s: %w", deviceName, err)
	}

	logger.Debugf("Successfully inserted efficiency metrics report for %s", deviceName)
	return nil
}

func insertSummaryData(data SummaryData, chdburl string, logger *logger.Logger) error {
	query := fmt.Sprintf(`
	INSERT INTO db_report.SummaryReport (
		Time, DeviceName, Count, EffectWindHours, Met1sWSpdAvg, Met1sWSpdMax, Met1sWSpdMin,
		Met30sWSpdAvg, Met30sWSpdMax, Met30sWSpdMin, Met600sWSpdAvg, Met600sWSpdMax, Met600sWSpdMin,
		MetAirDensityAvg, RatedPowerData, PreTotalGenerationFirst, GriActiveEnergyDelLast,
		GriActiveEnergyDelTotal, TheoreticalGeneration, PreTotalEleConsumptionFirst,
		GriActiveEnergyRcvLast, GriActiveEnergyRcvSection, GriActivePowerTotalAvg,
		GriActivePowerTotalMax, GriActivePowerTotalMin, GriReactivePowerTotalAvg,
		GriReactivePowerTotalMax, GriReactivePowerTotalMin, CnvGenPower30sAvg, CnvGenPower30sMax,
		CnvGenPower30sMin, CnvGenPower600sAvg, CnvGenPower600sMax, CnvGenPower600sMin,
		EquivalentHours, FullLoadDurationDay, GriActiveEnergyDelUnit, GriActiveEnergyRcvUnit,
		MetTmpAvg, MetTmpMax, MetTmpMin, NacTmpAvg, NacTmpMax, NacTmpMin, TowCbtTmpAvg,
		TowCbtTmpMax, TowCbtTmpMin, YawAcwHours, YawAcwTimes, YawCwHours, YawCwTimesSum,
		YawHours, GenReactQLimitStateDuration, GenReactQLimitStateTimesSum
	) VALUES (
		'%s', '%s', %d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f,
		%f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f,
		%f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f,
		%f, %f, %f
	)`,
		data.Time.Format("2006-01-02 15:04:05"), // Time as formatted string
		data.DeviceName,                         // DeviceName as escaped string
		data.Count,                              // uint64 as int
		data.EffectWindHours,                    // float64
		data.Met1sWSpdAvg,                       // float64
		data.Met1sWSpdMax,                       // float64
		data.Met1sWSpdMin,                       // float64
		data.Met30sWSpdAvg,                      // float64
		data.Met30sWSpdMax,                      // float64
		data.Met30sWSpdMin,                      // float64
		data.Met600sWSpdAvg,                     // float64
		data.Met600sWSpdMax,                     // float64
		data.Met600sWSpdMin,                     // float64
		data.MetAirDensityAvg,                   // float64
		data.RatedPowerData,                     // float64
		data.PreTotalGenerationFirst,            // float64
		data.GriActiveEnergyDelLast,             // float64
		data.GriActiveEnergyDelTotal,            // float64
		data.TheoreticalGeneration,              // float64
		data.PreTotalEleConsumptionFirst,        // float64
		data.GriActiveEnergyRcvLast,             // float64
		data.GriActiveEnergyRcvSection,          // float64
		data.GriActivePowerTotalAvg,             // float64
		data.GriActivePowerTotalMax,             // float64
		data.GriActivePowerTotalMin,             // float64
		data.GriReactivePowerTotalAvg,           // float64
		data.GriReactivePowerTotalMax,           // float64
		data.GriReactivePowerTotalMin,           // float64
		data.CnvGenPower30sAvg,                  // float64
		data.CnvGenPower30sMax,                  // float64
		data.CnvGenPower30sMin,                  // float64
		data.CnvGenPower600sAvg,                 // float64
		data.CnvGenPower600sMax,                 // float64
		data.CnvGenPower600sMin,                 // float64
		data.EquivalentHours,                    // float64
		data.FullLoadDurationDay,                // float64
		data.GriActiveEnergyDelUnit,             // float64
		data.GriActiveEnergyRcvUnit,             // float64
		data.MetTmpAvg,                          // float64
		data.MetTmpMax,                          // float64
		data.MetTmpMin,                          // float64
		data.NacTmpAvg,                          // float64
		data.NacTmpMax,                          // float64
		data.NacTmpMin,                          // float64
		data.TowCbtTmpAvg,                       // float64
		data.TowCbtTmpMax,                       // float64
		data.TowCbtTmpMin,                       // float64
		data.YawAcwHours,                        // float64
		data.YawAcwTimes,                        // float64
		data.YawCwHours,                         // float64
		data.YawCwTimesSum,                      // float64
		data.YawHours,                           // float64
		data.GenReactQLimitStateDuration,        // float64
		data.GenReactQLimitStateTimesSum,        // float64
	)

	_, err := doCHQuery(query, chdburl, logger)
	if err != nil {
		return fmt.Errorf("summary data query execution failed for %s: %w", data.DeviceName, err)
	}

	return nil
}

func insertTurbineAvailabilityMetrics(data TurbineAvailabilityMetrics, chdburl string, logger *logger.Logger) error {
	query := fmt.Sprintf(`INSERT INTO db_report.TurbineAvailabilityMetrics (

Time, DeviceName, Count, FaultShutdownDuration, OverhaulDuration, MaintenanceDuration,
GridShutdownDuration, LocalShutdownDuration, RemoteShutdownDuration, WeatherShutdownDuration,
LimitPowerDuration, LimitShutdownDuration, StandbyDuration, NormalGenerationDuration,
FaultShutdownTimes, OverhaulTimes, MaintenanceTimes, GridShutdownTimes, LocalShutdownTimes,
RemoteShutdownTimes, WeatherShutdownTimes, LimitPowerTimes, LimitShutdownTimes, StandbyTimes,
NormalGenerationTimes, InterruptionTimes, AvailableTime, UnAvailableTime,
Availability, InterruptionDurationCalculated, MTBFDuration, MTTRDuration
) VALUES (
'%s', '%s', %d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f
)`,

		data.Time.Format("2006-01-02 15:04:05"),
		data.DeviceName,
		data.Count,
		data.FaultShutdownDuration,
		data.OverhaulDuration,
		data.MaintenanceDuration,
		data.GridShutdownDuration,
		data.LocalShutdownDuration,
		data.RemoteShutdownDuration,
		data.WeatherShutdownDuration,
		data.LimitPowerDuration,
		data.LimitShutdownDuration,
		data.StandbyDuration,
		data.NormalGenerationDuration,
		data.FaultShutdownTimes,
		data.OverhaulTimes,
		data.MaintenanceTimes,
		data.GridShutdownTimes,
		data.LocalShutdownTimes,
		data.RemoteShutdownTimes,
		data.WeatherShutdownTimes,
		data.LimitPowerTimes,
		data.LimitShutdownTimes,
		data.StandbyTimes,
		data.NormalGenerationTimes,
		data.InterruptionTimes,
		data.AvailableTime,
		data.UnAvailableTime,
		data.Availability,
		data.InterruptionDurationCalculated,
		data.MTBFDuration,
		data.MTTRDuration,
	)

	_, err := doCHQuery(query, chdburl, logger)
	if err != nil {
		return fmt.Errorf("query execution failed for %s: %w", data.DeviceName, err)
	}

	return nil
}

func insertEfficiencyMetrics(data EfficiencyMetrics, chdburl string, logger *logger.Logger) error {
	query := fmt.Sprintf(`INSERT INTO db_report.EfficiencyMetrics (

Time, DeviceName, Count, Discrepancy, EnergyAvailability, FaultLossGeneration,
OverhaulLossGeneration, MaintainLossGeneration, GridLossGeneration, LocalLossGeneration,
RemoteLossGeneration, WeatherLossGeneration, LimitLossGeneration
) VALUES (
'%s', '%s', %d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f
)`,

		data.Time.Format("2006-01-02 15:04:05"),
		data.DeviceName,
		data.Count,
		data.Discrepancy,
		data.EnergyAvailability,
		data.FaultLossGeneration,
		data.OverhaulLossGeneration,
		data.MaintainLossGeneration,
		data.GridLossGeneration,
		data.LocalLossGeneration,
		data.RemoteLossGeneration,
		data.WeatherLossGeneration,
		data.LimitLossGeneration,
	)

	_, err := doCHQuery(query, chdburl, logger)
	if err != nil {
		return fmt.Errorf("query execution failed for %s: %w", data.DeviceName, err)
	}
	return nil
}

type SummaryData struct {
	Time                        time.Time `db:"Time" json:"time"`
	DeviceName                  string    `db:"DeviceName" json:"deviceName"`
	Count                       uint64    `db:"Count" json:"count,omitempty"`
	EffectWindHours             float64   `db:"EffectWindHours" json:"effectWindHours,omitempty"`
	Met1sWSpdAvg                float64   `db:"Met1sWSpdAvg" json:"met1sWSpdAvg,omitempty"`
	Met1sWSpdMax                float64   `db:"Met1sWSpdMax" json:"met1sWSpdMax,omitempty"`
	Met1sWSpdMin                float64   `db:"Met1sWSpdMin" json:"met1sWSpdMin,omitempty"`
	Met30sWSpdAvg               float64   `db:"Met30sWSpdAvg" json:"met30sWSpdAvg,omitempty"`
	Met30sWSpdMax               float64   `db:"Met30sWSpdMax" json:"met30sWSpdMax,omitempty"`
	Met30sWSpdMin               float64   `db:"Met30sWSpdMin" json:"met30sWSpdMin,omitempty"`
	Met600sWSpdAvg              float64   `db:"Met600sWSpdAvg" json:"met600sWSpdAvg,omitempty"`
	Met600sWSpdMax              float64   `db:"Met600sWSpdMax" json:"met600sWSpdMax,omitempty"`
	Met600sWSpdMin              float64   `db:"Met600sWSpdMin" json:"met600sWSpdMin,omitempty"`
	MetAirDensityAvg            float64   `db:"MetAirDensityAvg" json:"metAirDensityAvg,omitempty"`
	RatedPowerData              float64   `db:"RatedPowerData" json:"ratedPowerData,omitempty"`
	PreTotalGenerationFirst     float64   `db:"PreTotalGenerationFirst" json:"preTotalGenerationFirst,omitempty"`
	GriActiveEnergyDelLast      float64   `db:"GriActiveEnergyDelLast" json:"griActiveEnergyDelLast,omitempty"`
	GriActiveEnergyDelTotal     float64   `db:"GriActiveEnergyDelTotal" json:"griActiveEnergyDelTotal,omitempty"`
	TheoreticalGeneration       float64   `db:"TheoreticalGeneration" json:"theoreticalGeneration,omitempty"`
	PreTotalEleConsumptionFirst float64   `db:"PreTotalEleConsumptionFirst" json:"preTotalEleConsumptionFirst,omitempty"`
	GriActiveEnergyRcvLast      float64   `db:"GriActiveEnergyRcvLast" json:"griActiveEnergyRcvLast,omitempty"`
	GriActiveEnergyRcvSection   float64   `db:"GriActiveEnergyRcvSection" json:"griActiveEnergyRcvSection,omitempty"`
	GriActivePowerTotalAvg      float64   `db:"GriActivePowerTotalAvg" json:"griActivePowerTotalAvg,omitempty"`
	GriActivePowerTotalMax      float64   `db:"GriActivePowerTotalMax" json:"griActivePowerTotalMax,omitempty"`
	GriActivePowerTotalMin      float64   `db:"GriActivePowerTotalMin" json:"griActivePowerTotalMin,omitempty"`
	GriReactivePowerTotalAvg    float64   `db:"GriReactivePowerTotalAvg" json:"griReactivePowerTotalAvg,omitempty"`
	GriReactivePowerTotalMax    float64   `db:"GriReactivePowerTotalMax" json:"griReactivePowerTotalMax,omitempty"`
	GriReactivePowerTotalMin    float64   `db:"GriReactivePowerTotalMin" json:"griReactivePowerTotalMin,omitempty"`
	CnvGenPower30sAvg           float64   `db:"CnvGenPower30sAvg" json:"cnvGenPower30sAvg,omitempty"`
	CnvGenPower30sMax           float64   `db:"CnvGenPower30sMax" json:"cnvGenPower30sMax,omitempty"`
	CnvGenPower30sMin           float64   `db:"CnvGenPower30sMin" json:"cnvGenPower30sMin,omitempty"`
	CnvGenPower600sAvg          float64   `db:"CnvGenPower600sAvg" json:"cnvGenPower600sAvg,omitempty"`
	CnvGenPower600sMax          float64   `db:"CnvGenPower600sMax" json:"cnvGenPower600sMax,omitempty"`
	CnvGenPower600sMin          float64   `db:"CnvGenPower600sMin" json:"cnvGenPower600sMin,omitempty"`
	EquivalentHours             float64   `db:"EquivalentHours" json:"equivalentHours,omitempty"`
	FullLoadDurationDay         float64   `db:"FullLoadDurationDay" json:"fullLoadDurationDay,omitempty"`
	GriActiveEnergyDelUnit      float64   `db:"GriActiveEnergyDelUnit" json:"griActiveEnergyDelUnit,omitempty"`
	GriActiveEnergyRcvUnit      float64   `db:"GriActiveEnergyRcvUnit" json:"griActiveEnergyRcvUnit,omitempty"`
	MetTmpAvg                   float64   `db:"MetTmpAvg" json:"metTmpAvg,omitempty"`
	MetTmpMax                   float64   `db:"MetTmpMax" json:"metTmpMax,omitempty"`
	MetTmpMin                   float64   `db:"MetTmpMin" json:"metTmpMin,omitempty"`
	NacTmpAvg                   float64   `db:"NacTmpAvg" json:"nacTmpAvg,omitempty"`
	NacTmpMax                   float64   `db:"NacTmpMax" json:"nacTmpMax,omitempty"`
	NacTmpMin                   float64   `db:"NacTmpMin" json:"nacTmpMin,omitempty"`
	TowCbtTmpAvg                float64   `db:"TowCbtTmpAvg" json:"towCbtTmpAvg,omitempty"`
	TowCbtTmpMax                float64   `db:"TowCbtTmpMax" json:"towCbtTmpMax,omitempty"`
	TowCbtTmpMin                float64   `db:"TowCbtTmpMin" json:"towCbtTmpMin,omitempty"`
	YawAcwHours                 float64   `db:"YawAcwHours" json:"yawAcwHours,omitempty"`
	YawAcwTimes                 float64   `db:"YawAcwTimes" json:"yawAcwTimes,omitempty"`
	YawCwHours                  float64   `db:"YawCwHours" json:"yawCwHours,omitempty"`
	YawCwTimesSum               float64   `db:"YawCwTimesSum" json:"yawCwTimesSum,omitempty"`
	YawHours                    float64   `db:"YawHours" json:"yawHours,omitempty"`
	GenReactQLimitStateDuration float64   `db:"GenReactQLimitStateDuration" json:"genReactQLimitStateDuration,omitempty"`
	GenReactQLimitStateTimesSum float64   `db:"GenReactQLimitStateTimesSum" json:"genReactQLimitStateTimesSum,omitempty"`
}

type TurbineAvailabilityMetrics struct {
	Time                           time.Time `db:"Time" json:"time,omitempty"`
	DeviceName                     string    `db:"DeviceName" json:"deviceName,omitempty"`
	Count                          uint64    `db:"Count" json:"count,omitempty"`
	FaultShutdownDuration          float64   `db:"FaultShutdownDuration" json:"faultShutdownDuration,omitempty"`
	OverhaulDuration               float64   `db:"OverhaulDuration" json:"overhaulDuration,omitempty"`
	MaintenanceDuration            float64   `db:"MaintenanceDuration" json:"maintenanceDuration,omitempty"`
	GridShutdownDuration           float64   `db:"GridShutdownDuration" json:"gridShutdownDuration,omitempty"`
	LocalShutdownDuration          float64   `db:"LocalShutdownDuration" json:"localShutdownDuration,omitempty"`
	RemoteShutdownDuration         float64   `db:"RemoteShutdownDuration" json:"remoteShutdownDuration,omitempty"`
	WeatherShutdownDuration        float64   `db:"WeatherShutdownDuration" json:"weatherShutdownDuration,omitempty"`
	LimitPowerDuration             float64   `db:"LimitPowerDuration" json:"limitPowerDuration,omitempty"`
	LimitShutdownDuration          float64   `db:"LimitShutdownDuration" json:"limitShutdownDuration,omitempty"`
	StandbyDuration                float64   `db:"StandbyDuration" json:"standbyDuration,omitempty"`
	NormalGenerationDuration       float64   `db:"NormalGenerationDuration" json:"normalGenerationDuration,omitempty"`
	FaultShutdownTimes             float64   `db:"FaultShutdownTimes" json:"faultShutdownTimes,omitempty"`
	OverhaulTimes                  float64   `db:"OverhaulTimes" json:"overhaulTimes,omitempty"`
	MaintenanceTimes               float64   `db:"MaintenanceTimes" json:"maintenanceTimes,omitempty"`
	GridShutdownTimes              float64   `db:"GridShutdownTimes" json:"gridShutdownTimes,omitempty"`
	LocalShutdownTimes             float64   `db:"LocalShutdownTimes" json:"localShutdownTimes,omitempty"`
	RemoteShutdownTimes            float64   `db:"RemoteShutdownTimes" json:"remoteShutdownTimes,omitempty"`
	WeatherShutdownTimes           float64   `db:"WeatherShutdownTimes" json:"weatherShutdownTimes,omitempty"`
	LimitPowerTimes                float64   `db:"LimitPowerTimes" json:"limitPowerTimes,omitempty"`
	LimitShutdownTimes             float64   `db:"LimitShutdownTimes" json:"limitShutdownTimes,omitempty"`
	StandbyTimes                   float64   `db:"StandbyTimes" json:"standbyTimes,omitempty"`
	NormalGenerationTimes          float64   `db:"NormalGenerationTimes" json:"normalGenerationTimes,omitempty"`
	InterruptionTimes              float64   `db:"InterruptionTimes" json:"interruptionTimes,omitempty"`
	AvailableTime                  float64   `db:"AvailableTime" json:"availableTime,omitempty"`
	UnAvailableTime                float64   `db:"UnAvailableTime" json:"unAvailableTime,omitempty"`
	Availability                   float64   `db:"Availability" json:"availability,omitempty"`
	InterruptionDurationCalculated float64   `db:"InterruptionDuration_Calculated" json:"interruptionDurationCalculated,omitempty"`
	MTBFDuration                   float64   `db:"MTBFDuration" json:"mtbfDuration,omitempty"`
	MTTRDuration                   float64   `db:"MTTRDuration" json:"mttrDuration,omitempty"`
}

type EfficiencyMetrics struct {
	Time                   time.Time `db:"Time" json:"time,omitempty"`
	DeviceName             string    `db:"DeviceName" json:"deviceName,omitempty"`
	Count                  uint64    `db:"Count" json:"count,omitempty"`
	Discrepancy            float64   `db:"Discrepancy" json:"discrepancy,omitempty"`
	EnergyAvailability     float64   `db:"EnergyAvailability" json:"energyAvailability,omitempty"`
	FaultLossGeneration    float64   `db:"FaultLossGeneration" json:"faultLossGeneration,omitempty"`
	OverhaulLossGeneration float64   `db:"OverhaulLossGeneration" json:"overhaulLossGeneration,omitempty"`
	MaintainLossGeneration float64   `db:"MaintainLossGeneration" json:"maintainLossGeneration,omitempty"`
	GridLossGeneration     float64   `db:"GridLossGeneration" json:"gridLossGeneration,omitempty"`
	LocalLossGeneration    float64   `db:"LocalLossGeneration" json:"localLossGeneration,omitempty"`
	RemoteLossGeneration   float64   `db:"RemoteLossGeneration" json:"remoteLossGeneration,omitempty"`
	WeatherLossGeneration  float64   `db:"WeatherLossGeneration" json:"weatherLossGeneration,omitempty"`
	LimitLossGeneration    float64   `db:"LimitLossGeneration" json:"limitLossGeneration,omitempty"`
}

// CombinedReportData holds all the fields from the three reports for a single query.
type CombinedReportData struct {
	// Fields from SummaryData
	Time                        time.Time `json:"Time"`
	DeviceName                  string    `json:"DeviceName"`
	Count                       uint64    `json:"Count"`
	EffectWindHours             float64   `json:"EffectWindHours"`
	Met1sWSpdAvg                float64   `json:"Met1sWSpdAvg"`
	Met1sWSpdMax                float64   `json:"Met1sWSpdMax"`
	Met1sWSpdMin                float64   `json:"Met1sWSpdMin"`
	Met30sWSpdAvg               float64   `json:"Met30sWSpdAvg"`
	Met30sWSpdMax               float64   `json:"Met30sWSpdMax"`
	Met30sWSpdMin               float64   `json:"Met30sWSpdMin"`
	Met600sWSpdAvg              float64   `json:"Met600sWSpdAvg"`
	Met600sWSpdMax              float64   `json:"Met600sWSpdMax"`
	Met600sWSpdMin              float64   `json:"Met600sWSpdMin"`
	MetAirDensityAvg            float64   `json:"MetAirDensityAvg"`
	EquivalentHours             float64   `json:"EquivalentHours"`
	RatedPowerData              float64   `json:"RatedPowerData"`
	PreTotalGenerationFirst     float64   `json:"PreTotalGenerationFirst"`
	GriActiveEnergyDelLast      float64   `json:"GriActiveEnergyDelLast"`
	GriActiveEnergyDelTotal     float64   `json:"GriActiveEnergyDelTotal"`
	TheoreticalGeneration       float64   `json:"TheoreticalGeneration"`
	PreTotalEleConsumptionFirst float64   `json:"PreTotalEleConsumptionFirst"`
	GriActiveEnergyRcvLast      float64   `json:"GriActiveEnergyRcvLast"`
	GriActiveEnergyRcvSection   float64   `json:"GriActiveEnergyRcvSection"`
	GriActivePowerTotalAvg      float64   `json:"GriActivePowerTotalAvg"`
	GriActivePowerTotalMax      float64   `json:"GriActivePowerTotalMax"`
	GriActivePowerTotalMin      float64   `json:"GriActivePowerTotalMin"`
	GriReactivePowerTotalAvg    float64   `json:"GriReactivePowerTotalAvg"`
	GriReactivePowerTotalMax    float64   `json:"GriReactivePowerTotalMax"`
	GriReactivePowerTotalMin    float64   `json:"GriReactivePowerTotalMin"`
	CnvGenPower30sAvg           float64   `json:"CnvGenPower30sAvg"`
	CnvGenPower30sMax           float64   `json:"CnvGenPower30sMax"`
	CnvGenPower30sMin           float64   `json:"CnvGenPower30sMin"`
	CnvGenPower600sAvg          float64   `json:"CnvGenPower600sAvg"`
	CnvGenPower600sMax          float64   `json:"CnvGenPower600sMax"`
	CnvGenPower600sMin          float64   `json:"CnvGenPower600sMin"`
	FullLoadDurationDay         float64   `json:"FullLoadDurationDay"`
	GriActiveEnergyDelUnit      float64   `json:"GriActiveEnergyDelUnit"`
	GriActiveEnergyRcvUnit      float64   `json:"GriActiveEnergyRcvUnit"`
	MetTmpAvg                   float64   `json:"MetTmpAvg"`
	MetTmpMax                   float64   `json:"MetTmpMax"`
	MetTmpMin                   float64   `json:"MetTmpMin"`
	NacTmpAvg                   float64   `json:"NacTmpAvg"`
	NacTmpMax                   float64   `json:"NacTmpMax"`
	NacTmpMin                   float64   `json:"NacTmpMin"`
	TowCbtTmpAvg                float64   `json:"TowCbtTmpAvg"`
	TowCbtTmpMax                float64   `json:"TowCbtTmpMax"`
	TowCbtTmpMin                float64   `json:"TowCbtTmpMin"`
	YawAcwHours                 float64   `json:"YawAcwHours"`
	YawAcwTimes                 float64   `json:"YawAcwTimes"`
	YawCwHours                  float64   `json:"YawCwHours"`
	YawCwTimesSum               float64   `json:"YawCwTimesSum"`
	YawHours                    float64   `json:"YawHours"`
	GenReactQLimitStateDuration float64   `json:"GenReactQLimitStateDuration"`
	GenReactQLimitStateTimesSum float64   `json:"GenReactQLimitStateTimesSum"`

	// Fields from TurbineAvailabilityMetrics
	FaultShutdownDuration          float64 `json:"FaultShutdownDuration"`
	OverhaulDuration               float64 `json:"OverhaulDuration"`
	MaintenanceDuration            float64 `json:"MaintenanceDuration"`
	GridShutdownDuration           float64 `json:"GridShutdownDuration"`
	LocalShutdownDuration          float64 `json:"LocalShutdownDuration"`
	RemoteShutdownDuration         float64 `json:"RemoteShutdownDuration"`
	WeatherShutdownDuration        float64 `json:"WeatherShutdownDuration"`
	LimitPowerDuration             float64 `json:"LimitPowerDuration"`
	LimitShutdownDuration          float64 `json:"LimitShutdownDuration"`
	StandbyDuration                float64 `json:"StandbyDuration"`
	NormalGenerationDuration       float64 `json:"NormalGenerationDuration"`
	FaultShutdownTimes             float64 `json:"FaultShutdownTimes"`
	OverhaulTimes                  float64 `json:"OverhaulTimes"`
	MaintenanceTimes               float64 `json:"MaintenanceTimes"`
	GridShutdownTimes              float64 `json:"GridShutdownTimes"`
	LocalShutdownTimes             float64 `json:"LocalShutdownTimes"`
	RemoteShutdownTimes            float64 `json:"RemoteShutdownTimes"`
	WeatherShutdownTimes           float64 `json:"WeatherShutdownTimes"`
	LimitPowerTimes                float64 `json:"LimitPowerTimes"`
	LimitShutdownTimes             float64 `json:"LimitShutdownTimes"`
	StandbyTimes                   float64 `json:"StandbyTimes"`
	NormalGenerationTimes          float64 `json:"NormalGenerationTimes"`
	InterruptionTimes              float64 `json:"InterruptionTimes"`
	AvailableTime                  float64 `json:"AvailableTime"`
	UnAvailableTime                float64 `json:"UnAvailableTime"`
	Availability                   float64 `json:"Availability"`
	InterruptionDurationCalculated float64 `json:"InterruptionDuration_Calculated"`
	MTBFDuration                   float64 `json:"MTBFDuration"`
	MTTRDuration                   float64 `json:"MTTRDuration"`

	// Fields from EfficiencyMetrics
	Discrepancy            float64 `json:"Discrepancy"`
	EnergyAvailability     float64 `json:"EnergyAvailability"`
	FaultLossGeneration    float64 `json:"FaultLossGeneration"`
	OverhaulLossGeneration float64 `json:"OverhaulLossGeneration"`
	MaintainLossGeneration float64 `json:"MaintainLossGeneration"`
	GridLossGeneration     float64 `json:"GridLossGeneration"`
	LocalLossGeneration    float64 `json:"LocalLossGeneration"`
	RemoteLossGeneration   float64 `json:"RemoteLossGeneration"`
	WeatherLossGeneration  float64 `json:"WeatherLossGeneration"`
	LimitLossGeneration    float64 `json:"LimitLossGeneration"`
}

// CalculateAndInsertCombinedReport fetches and processes all report data in a single query for efficiency.
func CalculateAndInsertCombinedReport(filePath, deviceName string, logger *logger.Logger, chdburl string) error {
	// Step 1: Build the unified SQL query using CTEs.
	buildSQL := func(fp string) string {
		// This single query combines all the logic from the three original queries.
		return fmt.Sprintf(`
WITH
    SourceDataWithRowNumbers AS (
        SELECT
            *,
            ROW_NUMBER() OVER (ORDER BY time DESC) AS rn_desc,
            ROW_NUMBER() OVER (ORDER BY time ASC) AS rn_asc
        FROM file('%s', Parquet)
    ),
    AggregatedDataBase AS (
        SELECT
            COUNT(MC082) AS Count,
            SUM(IsEffectWind) AS EffectWindHours,
            AVG(MC004) AS Met1sWSpdAvg, MAX(MC004) AS Met1sWSpdMax, MIN(MC004) AS Met1sWSpdMin,
            AVG(MC082) AS Met30sWSpdAvg, MAX(MC082) AS Met30sWSpdMax, MIN(MC082) AS Met30sWSpdMin,
            AVG(MC083) AS Met600sWSpdAvg, MAX(MC083) AS Met600sWSpdMax, MIN(MC083) AS Met600sWSpdMin,
            AVG(MC093) AS MetAirDensityAvg,
            MIN(RatedPower) AS RatedPower1,
            MIN(MC006) AS MC006_Min,
            MAX(MC006) AS MC006_Max,
            MIN(MC062) AS PreTotalEleConsumptionFirst,
            MAX(MC062) AS GriActiveEnergyRcvLast,
            AVG(MC061) AS GriActivePowerTotalAvg, MAX(MC061) AS GriActivePowerTotalMax, MIN(MC061) AS GriActivePowerTotalMin,
            AVG(MD002) AS GriReactivePowerTotalAvg, MAX(MD002) AS GriReactivePowerTotalMax, MIN(MD002) AS GriReactivePowerTotalMin,
            AVG(MC085) AS CnvGenPower30sAvg, MAX(MC085) AS CnvGenPower30sMax, MIN(MC085) AS CnvGenPower30sMin,
            AVG(MC086) AS CnvGenPower600sAvg, MAX(MC086) AS CnvGenPower600sMax, MIN(MC086) AS CnvGenPower600sMin,
            COUNT(CASE WHEN MD001 >= RatedPower THEN 1 END) AS FullLoadDurationDay,
            SUM(Generation) / 10000.0 AS GriActiveEnergyDelUnit,
            SUM(Consumption) / 10000.0 AS GriActiveEnergyRcvUnit,
            AVG(MC015) AS MetTmpAvg, MAX(MC015) AS MetTmpMax, MIN(MC015) AS MetTmpMin,
            AVG(MC014) AS NacTmpAvg, MAX(MC014) AS NacTmpMax, MIN(MC014) AS NacTmpMin,
            AVG(MC091) AS TowCbtTmpAvg, MAX(MC091) AS TowCbtTmpMax, MIN(MC091) AS TowCbtTmpMin,
            SUM(CASE WHEN MA022 = 1 THEN 1 ELSE 0 END) AS YawAcwHours,
            SUM(CASE WHEN YawCcwTimes = 1 THEN 1 ELSE 0 END) AS YawAcwTimes,
            SUM(CASE WHEN MA021 = 1 THEN 1 ELSE 0 END) AS YawCwHours,
            SUM(YawCwTimes) AS YawCwTimesSum,
            SUM(CASE WHEN MA2353 = 1 THEN 1 ELSE 0 END) AS GenReactQLimitStateDuration,
            SUM(GenReactQLimitStateTimes) AS GenReactQLimitStateTimesSum,

            SUM(CASE WHEN MC143 = 40 THEN 1 ELSE 0 END) AS FaultShutdownDuration,
            SUM(CASE WHEN MC143 = 50 THEN 1 ELSE 0 END) AS OverhaulDuration,
            SUM(CASE WHEN MC143 = 60 THEN 1 ELSE 0 END) AS MaintenanceDuration,
            SUM(CASE WHEN MC143 = 70 THEN 1 ELSE 0 END) AS GridShutdownDuration,
            SUM(CASE WHEN MC143 = 81 THEN 1 ELSE 0 END) AS LocalShutdownDuration,
            SUM(CASE WHEN MC143 = 80 THEN 1 ELSE 0 END) AS RemoteShutdownDuration,
            SUM(CASE WHEN MC143 = 90 THEN 1 ELSE 0 END) AS WeatherShutdownDuration,
            SUM(CASE WHEN MC143 = 110 THEN 1 ELSE 0 END) AS LimitPowerDuration,
            SUM(CASE WHEN MC143 = 111 THEN 1 ELSE 0 END) AS LimitShutdownDuration,
            SUM(CASE WHEN MC143 = 100 THEN 1 ELSE 0 END) AS StandbyDuration,
            SUM(CASE WHEN MC143 = 120 THEN 1 ELSE 0 END) AS NormalGenerationDuration,
            SUM(FaultShutdownTimes) AS FaultShutdownTimes,
            SUM(OverhaulTimes) AS OverhaulTimes,
            SUM(MaintenanceTimes) AS MaintenanceTimes,
            SUM(GridShutdownTimes) AS GridShutdownTimes,
            SUM(LocalShutdownTimes) AS LocalShutdownTimes,
            SUM(RemoteShutdownTimes) AS RemoteShutdownTimes,
            SUM(WeatherShutdownTimes) AS WeatherShutdownTimes,
            SUM(LimitPowerTimes) AS LimitPowerTimes,
            SUM(LimitShutdownTimes) AS LimitShutdownTimes,
            SUM(StandbyTimes) AS StandbyTimes,
            SUM(NormalGenerationTimes) AS NormalGenerationTimes,
            SUM(InterruptionTimes) AS InterruptionTimes,
            COUNT(*) AS TotalCount,

            MAX(CASE WHEN rn_desc = 1 THEN MC006 END) AS latest_mc006,
            MAX(CASE WHEN rn_asc = 1 THEN preTotalGeneration END) AS earliest_preTotalGeneration,
            SUM(FaultLossGeneration) AS sum_fault_loss,
            SUM(OverhaulLossGeneration) AS sum_overhaul_loss,
            SUM(MaintainLossGeneration) AS sum_maintain_loss,
            SUM(GridLossGeneration) AS sum_grid_loss,
            SUM(RemoteLossGeneration) AS sum_remote_loss,
            SUM(LocalLossGeneration) AS sum_local_loss,
            SUM(WeatherLossGeneration) AS sum_weather_loss,
            SUM(LimitLossGeneration) AS sum_limit_loss,
            SUM(TheoreticalGeneration) AS sum_theoretical_gen
        FROM SourceDataWithRowNumbers
    )
SELECT
    *,
    (MC006_Max - MC006_Min) / NULLIF(RatedPower1, 0) * 3600 AS EquivalentHours,
    RatedPower1 AS RatedPowerData,
    MC006_Min AS PreTotalGenerationFirst,
    MC006_Max AS GriActiveEnergyDelLast,
    (MC006_Max - MC006_Min) AS GriActiveEnergyDelTotal,
    sum_theoretical_gen AS TheoreticalGeneration,
    (GriActiveEnergyRcvLast - PreTotalEleConsumptionFirst) AS GriActiveEnergyRcvSection,
    (YawAcwHours + YawCwHours) AS YawHours,

    (MaintenanceDuration + GridShutdownDuration + LocalShutdownDuration + RemoteShutdownDuration + WeatherShutdownDuration + LimitPowerDuration + LimitShutdownDuration + StandbyDuration + NormalGenerationDuration) AS AvailableTime,
    (FaultShutdownDuration + OverhaulDuration) AS UnAvailableTime,
    (1.0 - (FaultShutdownDuration + OverhaulDuration) * 1.0 / NULLIF(TotalCount, 0)) AS Availability,
    (600 - TotalCount) AS InterruptionDuration_Calculated,
    ((TotalCount - FaultShutdownDuration) * 1.0 / NULLIF(FaultShutdownTimes, 0)) AS MTBFDuration,
    (FaultShutdownDuration * 1.0 / NULLIF(FaultShutdownTimes, 0)) AS MTTRDuration,

    (latest_mc006 - earliest_preTotalGeneration) AS actual_generation_delta,
    (sum_fault_loss + sum_overhaul_loss + sum_maintain_loss + sum_grid_loss + sum_remote_loss + sum_local_loss + sum_weather_loss) / 3600.0 AS total_non_limit_loss_div_3600,
    ((latest_mc006 - earliest_preTotalGeneration) / NULLIF(sum_theoretical_gen, 0)) * 3600.0 * 100.0 AS EnergyAvailability,
    sum_fault_loss / 3600.0 / 10000.0 AS FaultLossGeneration,
    sum_overhaul_loss / 3600.0 / 10000.0 AS OverhaulLossGeneration,
    sum_maintain_loss / 3600.0 / 10000.0 AS MaintainLossGeneration,
    sum_grid_loss / 3600.0 / 10000.0 AS GridLossGeneration,
    sum_local_loss / 3600.0 / 10000.0 AS LocalLossGeneration,
    sum_remote_loss / 3600.0 / 10000.0 AS RemoteLossGeneration,
    sum_weather_loss / 3600.0 / 10000.0 AS WeatherLossGeneration,
    sum_limit_loss / 3600.0 / 10000.0 AS LimitLossGeneration,
	(1.0 - ((latest_mc006 - earliest_preTotalGeneration) /
            NULLIF((latest_mc006 - earliest_preTotalGeneration) + ((sum_fault_loss + sum_overhaul_loss + sum_maintain_loss + sum_grid_loss + sum_remote_loss + sum_local_loss + sum_weather_loss) / 3600.0), 0))) * 100.0 AS Discrepancy

FROM AggregatedDataBase
`, fp)
	}

	// Step 2: Define a setter to populate common fields.
	setter := func(report *CombinedReportData, devName string, rTime time.Time) {
		report.DeviceName = devName
		report.Time = rTime
	}

	// Step 3: Fetch the combined data using the generic helper.
	report, err := fetchAndPrepareReportData[CombinedReportData](filePath, deviceName, chdburl, logger, buildSQL, setter)
	if err != nil {
		return fmt.Errorf("failed to fetch and prepare combined report data for %s: %w", deviceName, err)
	}
	if report == nil {
		logger.Infof("No combined data to insert for file %s, device %s", filePath, deviceName)
		return nil
	}

	// Step 4: Populate the individual structs from the combined report.
	summaryReport := SummaryData{
		Time:                        report.Time,
		DeviceName:                  report.DeviceName,
		Count:                       report.Count,
		EffectWindHours:             report.EffectWindHours,
		Met1sWSpdAvg:                report.Met1sWSpdAvg,
		Met1sWSpdMax:                report.Met1sWSpdMax,
		Met1sWSpdMin:                report.Met1sWSpdMin,
		Met30sWSpdAvg:               report.Met30sWSpdAvg,
		Met30sWSpdMax:               report.Met30sWSpdMax,
		Met30sWSpdMin:               report.Met30sWSpdMin,
		Met600sWSpdAvg:              report.Met600sWSpdAvg,
		Met600sWSpdMax:              report.Met600sWSpdMax,
		Met600sWSpdMin:              report.Met600sWSpdMin,
		MetAirDensityAvg:            report.MetAirDensityAvg,
		RatedPowerData:              report.RatedPowerData,
		PreTotalGenerationFirst:     report.PreTotalGenerationFirst,
		GriActiveEnergyDelLast:      report.GriActiveEnergyDelLast,
		GriActiveEnergyDelTotal:     report.GriActiveEnergyDelTotal,
		TheoreticalGeneration:       report.TheoreticalGeneration,
		PreTotalEleConsumptionFirst: report.PreTotalEleConsumptionFirst,
		GriActiveEnergyRcvLast:      report.GriActiveEnergyRcvLast,
		GriActiveEnergyRcvSection:   report.GriActiveEnergyRcvSection,
		GriActivePowerTotalAvg:      report.GriActivePowerTotalAvg,
		GriActivePowerTotalMax:      report.GriActivePowerTotalMax,
		GriActivePowerTotalMin:      report.GriActivePowerTotalMin,
		GriReactivePowerTotalAvg:    report.GriReactivePowerTotalAvg,
		GriReactivePowerTotalMax:    report.GriReactivePowerTotalMax,
		GriReactivePowerTotalMin:    report.GriReactivePowerTotalMin,
		CnvGenPower30sAvg:           report.CnvGenPower30sAvg,
		CnvGenPower30sMax:           report.CnvGenPower30sMax,
		CnvGenPower30sMin:           report.CnvGenPower30sMin,
		CnvGenPower600sAvg:          report.CnvGenPower600sAvg,
		CnvGenPower600sMax:          report.CnvGenPower600sMax,
		CnvGenPower600sMin:          report.CnvGenPower600sMin,
		EquivalentHours:             report.EquivalentHours,
		FullLoadDurationDay:         report.FullLoadDurationDay,
		GriActiveEnergyDelUnit:      report.GriActiveEnergyDelUnit,
		GriActiveEnergyRcvUnit:      report.GriActiveEnergyRcvUnit,
		MetTmpAvg:                   report.MetTmpAvg,
		MetTmpMax:                   report.MetTmpMax,
		MetTmpMin:                   report.MetTmpMin,
		NacTmpAvg:                   report.NacTmpAvg,
		NacTmpMax:                   report.NacTmpMax,
		NacTmpMin:                   report.NacTmpMin,
		TowCbtTmpAvg:                report.TowCbtTmpAvg,
		TowCbtTmpMax:                report.TowCbtTmpMax,
		TowCbtTmpMin:                report.TowCbtTmpMin,
		YawAcwHours:                 report.YawAcwHours,
		YawAcwTimes:                 report.YawAcwTimes,
		YawCwHours:                  report.YawCwHours,
		YawCwTimesSum:               report.YawCwTimesSum,
		YawHours:                    report.YawHours,
		GenReactQLimitStateDuration: report.GenReactQLimitStateDuration,
		GenReactQLimitStateTimesSum: report.GenReactQLimitStateTimesSum,
	}

	availabilityReport := TurbineAvailabilityMetrics{
		Time:                           report.Time,
		DeviceName:                     report.DeviceName,
		Count:                          report.Count,
		FaultShutdownDuration:          report.FaultShutdownDuration,
		OverhaulDuration:               report.OverhaulDuration,
		MaintenanceDuration:            report.MaintenanceDuration,
		GridShutdownDuration:           report.GridShutdownDuration,
		LocalShutdownDuration:          report.LocalShutdownDuration,
		RemoteShutdownDuration:         report.RemoteShutdownDuration,
		WeatherShutdownDuration:        report.WeatherShutdownDuration,
		LimitPowerDuration:             report.LimitPowerDuration,
		LimitShutdownDuration:          report.LimitShutdownDuration,
		StandbyDuration:                report.StandbyDuration,
		NormalGenerationDuration:       report.NormalGenerationDuration,
		FaultShutdownTimes:             report.FaultShutdownTimes,
		OverhaulTimes:                  report.OverhaulTimes,
		MaintenanceTimes:               report.MaintenanceTimes,
		GridShutdownTimes:              report.GridShutdownTimes,
		LocalShutdownTimes:             report.LocalShutdownTimes,
		RemoteShutdownTimes:            report.RemoteShutdownTimes,
		WeatherShutdownTimes:           report.WeatherShutdownTimes,
		LimitPowerTimes:                report.LimitPowerTimes,
		LimitShutdownTimes:             report.LimitShutdownTimes,
		StandbyTimes:                   report.StandbyTimes,
		NormalGenerationTimes:          report.NormalGenerationTimes,
		InterruptionTimes:              report.InterruptionTimes,
		AvailableTime:                  report.AvailableTime,
		UnAvailableTime:                report.UnAvailableTime,
		Availability:                   report.Availability,
		InterruptionDurationCalculated: report.InterruptionDurationCalculated,
		MTBFDuration:                   report.MTBFDuration,
		MTTRDuration:                   report.MTTRDuration,
	}

	efficiencyReport := EfficiencyMetrics{
		Time:                   report.Time,
		DeviceName:             report.DeviceName,
		Count:                  report.Count,
		Discrepancy:            report.Discrepancy,
		EnergyAvailability:     report.EnergyAvailability,
		FaultLossGeneration:    report.FaultLossGeneration,
		OverhaulLossGeneration: report.OverhaulLossGeneration,
		MaintainLossGeneration: report.MaintainLossGeneration,
		GridLossGeneration:     report.GridLossGeneration,
		LocalLossGeneration:    report.LocalLossGeneration,
		RemoteLossGeneration:   report.RemoteLossGeneration,
		WeatherLossGeneration:  report.WeatherLossGeneration,
		LimitLossGeneration:    report.LimitLossGeneration,
	}

	// Step 5: Insert the data into their respective tables.
	if err := insertSummaryData(summaryReport, chdburl, logger); err != nil {
		return fmt.Errorf("error inserting summary data for %s: %w", deviceName, err)
	}
	logger.Debugf("Successfully inserted summary report for %s", deviceName)

	if err := insertTurbineAvailabilityMetrics(availabilityReport, chdburl, logger); err != nil {
		return fmt.Errorf("error inserting turbine availability report for %s: %w", deviceName, err)
	}
	logger.Debugf("Successfully inserted turbine availability report for %s", deviceName)

	if err := insertEfficiencyMetrics(efficiencyReport, chdburl, logger); err != nil {
		return fmt.Errorf("error inserting efficiency metrics report for %s: %w", deviceName, err)
	}
	logger.Debugf("Successfully inserted efficiency metrics report for %s", deviceName)

	return nil
}
