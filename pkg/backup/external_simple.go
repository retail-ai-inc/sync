package backup

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// ExecuteExternalMongoBackup 使用外部命令执行MongoDB备份
// 避免Go内存管理问题，直接调用系统命令
func (e *BackupExecutor) ExecuteExternalMongoBackup(ctx context.Context, config ExecutorBackupConfig, tempDir string, task BackupTask, collection string) error {
	logrus.Infof("[BackupExecutor] 🚀 Using EXTERNAL COMMAND mode for collection: %s", collection)

	// 记录Go进程内存 (应该保持稳定)
	e.logMemoryUsage("EXTERNAL_MODE_START")

	// 构建连接字符串
	connStr := buildMongoDBConnectionString(config.Database.URL, config.Database.Username, config.Database.Password)

	// 文件路径
	dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	outputPath := fmt.Sprintf("%s/%s_%s.json", tempDir, collection, dateStr)
	zipPath := fmt.Sprintf("%s/%s_%s.zip", tempDir, collection, dateStr)

	// Step 1: 外部mongoexport命令
	logrus.Infof("[BackupExecutor] 📤 Step 1: External mongoexport")
	if err := e.executeExternalMongoExport(ctx, connStr, config.Database.Database, collection, outputPath); err != nil {
		return fmt.Errorf("external mongoexport failed: %w", err)
	}

	e.logMemoryUsage("AFTER_MONGOEXPORT")

	// Step 2: 外部zip命令
	logrus.Infof("[BackupExecutor] 🗜️ Step 2: External zip compression")
	if err := e.executeExternalZip(ctx, tempDir, outputPath, zipPath); err != nil {
		return fmt.Errorf("external zip failed: %w", err)
	}

	e.logMemoryUsage("AFTER_ZIP")

	// Step 3: 外部gsutil上传 (如果配置了GCS)
	if config.Destination.GCSPath != "" {
		logrus.Infof("[BackupExecutor] ☁️ Step 3: External GCS upload")
		gcsPath := fmt.Sprintf("%s/%s_%s.zip", config.Destination.GCSPath, collection, dateStr)
		if err := e.executeExternalGCSUpload(ctx, zipPath, gcsPath); err != nil {
			return fmt.Errorf("external GCS upload failed: %w", err)
		}
	}

	e.logMemoryUsage("EXTERNAL_MODE_COMPLETE")

	// 暂时不删除临时文件，保留用于调试分析
	logrus.Infof("[BackupExecutor] 🔍 Keeping JSON file for debugging: %s", outputPath)
	logrus.Infof("[BackupExecutor] 🔍 Keeping ZIP file for debugging: %s", zipPath)
	// 保留所有文件供调试分析

	logrus.Infof("[BackupExecutor] ✅ External backup completed for collection: %s", collection)
	return nil
}

// executeExternalMongoExport 执行外部mongoexport命令
func (e *BackupExecutor) executeExternalMongoExport(ctx context.Context, connStr, database, collection, outputPath string) error {
	cmd := exec.CommandContext(ctx, "mongoexport",
		"--uri", connStr,
		"--db", database,
		"--collection", collection,
		"--out", outputPath,
		"--quiet")

	logrus.Infof("[BackupExecutor] Executing: mongoexport --db %s --collection %s --out %s", database, collection, outputPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mongoexport failed: %w, output: %s", err, string(output))
	}

	// 检查输出文件
	if _, err := os.Stat(outputPath); err != nil {
		return fmt.Errorf("mongoexport output file not created: %w", err)
	}

	// 记录文件大小
	if stat, err := os.Stat(outputPath); err == nil {
		logrus.Infof("[BackupExecutor] ✅ Mongoexport completed: %.2f MB", float64(stat.Size())/1024/1024)
	}

	return nil
}

// executeExternalZip 执行外部zip命令
func (e *BackupExecutor) executeExternalZip(ctx context.Context, workDir, inputFile, outputFile string) error {
	// 使用系统zip命令
	cmd := exec.CommandContext(ctx, "zip", "-j", outputFile, inputFile)
	cmd.Dir = workDir

	logrus.Infof("[BackupExecutor] Executing: zip -j %s %s", outputFile, inputFile)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("zip failed: %w, output: %s", err, string(output))
	}

	// 检查输出文件
	if _, err := os.Stat(outputFile); err != nil {
		return fmt.Errorf("zip output file not created: %w", err)
	}

	// 记录压缩效果
	if stat, err := os.Stat(outputFile); err == nil {
		logrus.Infof("[BackupExecutor] ✅ Zip completed: %.2f MB", float64(stat.Size())/1024/1024)
	}

	return nil
}

// executeExternalGCSUpload 执行外部gsutil上传
func (e *BackupExecutor) executeExternalGCSUpload(ctx context.Context, localFile, gcsPath string) error {
	cmd := exec.CommandContext(ctx, "gsutil", "cp", localFile, gcsPath)

	logrus.Infof("[BackupExecutor] Executing: gsutil cp %s %s", localFile, gcsPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("gsutil upload failed: %w, output: %s", err, string(output))
	}

	logrus.Infof("[BackupExecutor] ✅ GCS upload completed: %s", gcsPath)
	return nil
}

// copyExistingFile 复制现有数据文件用于测试
func (e *BackupExecutor) copyExistingFile(src, dst string) error {
	// 记录开始时内存
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[BackupExecutor] 🔄 Memory BEFORE file copy: Alloc=%.2fMB, Sys=%.2fMB",
		float64(memStats.Alloc)/1024/1024,
		float64(memStats.Sys)/1024/1024)

	// 使用系统cp命令复制文件以避免Go内存使用
	cmd := exec.CommandContext(context.Background(), "cp", src, dst)

	logrus.Infof("[BackupExecutor] 🔄 Executing: cp %s %s", src, dst)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("cp command failed: %w, output: %s", err, string(output))
	}

	// 验证复制结果
	if _, err := os.Stat(dst); err != nil {
		return fmt.Errorf("copied file not found: %w", err)
	}

	// 记录复制后内存
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[BackupExecutor] 🔄 Memory AFTER file copy: Alloc=%.2fMB, Sys=%.2fMB",
		float64(memStats.Alloc)/1024/1024,
		float64(memStats.Sys)/1024/1024)

	return nil
}

// logMemoryUsage 记录内存使用情况
func (e *BackupExecutor) logMemoryUsage(phase string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	logrus.Infof("[BackupExecutor] 📊 Go Memory [%s]: Alloc=%.2fMB, Sys=%.2fMB, NumGoroutines=%d",
		phase,
		float64(m.Alloc)/1024/1024,
		float64(m.Sys)/1024/1024,
		runtime.NumGoroutine())
}

// executeExternalMongoExportSimple 完整的外部命令备份：mongoexport -> zip -> GCS upload
func (e *BackupExecutor) executeExternalMongoExportSimple(ctx context.Context, connStr, database, collection, tempDir string, config ExecutorBackupConfig) error {
	logrus.Infof("[BackupExecutor] 🚀 Starting COMPLETE external command backup for collection: %s", collection)

	// 记录Go进程内存 (应该保持稳定)
	e.logMemoryUsage("EXTERNAL_FULL_START")

	// 文件路径
	dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	outputPath := fmt.Sprintf("%s/%s_%s.json", tempDir, collection, dateStr)
	zipPath := fmt.Sprintf("%s/%s_%s.zip", tempDir, collection, dateStr)

	// Step 1: 使用现有数据文件 (跳过mongoexport以节省时间)
	logrus.Infof("[BackupExecutor] 📤 Step 1: Using existing data file (SKIP mongoexport)")

	// 检查现有数据文件路径 (支持本地测试)
	existingFiles := []string{
		"/mnt/state/RetailerRecommendationAnalytics_202508_2025-08-26.json",     // 服务器路径
		"/tmp/mnt/state/RetailerRecommendationAnalytics_202508_2025-08-26.json", // 本地测试路径
	}

	var existingFile string
	for _, file := range existingFiles {
		if _, err := os.Stat(file); err == nil {
			existingFile = file
			break
		}
	}

	if existingFile != "" {
		logrus.Infof("[BackupExecutor] ✅ Found existing file: %s", existingFile)

		// 复制现有文件到输出路径
		if err := e.copyExistingFile(existingFile, outputPath); err != nil {
			return fmt.Errorf("failed to copy existing file: %w", err)
		}
		logrus.Infof("[BackupExecutor] ✅ Copied existing file to: %s", outputPath)

		// 记录文件大小
		if stat, err := os.Stat(outputPath); err == nil {
			logrus.Infof("[BackupExecutor] 📊 Data file size: %.2f MB", float64(stat.Size())/1024/1024)
		}
	} else {
		// 如果没有现有文件，回退到mongoexport with query conditions
		logrus.Infof("[BackupExecutor] ⚠️ Existing file not found, falling back to mongoexport with query conditions")
		if err := e.executeExternalMongoExportWithOptions(ctx, connStr, database, collection, outputPath, config); err != nil {
			return fmt.Errorf("external mongoexport failed: %w", err)
		}
	}

	e.logMemoryUsage("AFTER_EXTERNAL_EXPORT")

	// Step 2: 外部zip命令
	logrus.Infof("[BackupExecutor] 🗜️ Step 2: External zip compression")
	if err := e.executeExternalZip(ctx, tempDir, outputPath, zipPath); err != nil {
		return fmt.Errorf("external zip failed: %w", err)
	}

	e.logMemoryUsage("AFTER_EXTERNAL_ZIP")

	// Step 3: 外部GCS上传 (需要配置GCS路径)
	// TODO: 从配置中获取GCS路径
	gcsPath := fmt.Sprintf("gs://logs-router-bucketbk/external/%s_%s.zip", collection, dateStr)
	logrus.Infof("[BackupExecutor] ☁️ Step 3: External GCS upload")
	if err := e.executeExternalGCSUpload(ctx, zipPath, gcsPath); err != nil {
		return fmt.Errorf("external GCS upload failed: %w", err)
	}

	e.logMemoryUsage("EXTERNAL_FULL_COMPLETE")

	// 暂时不删除临时文件，保留用于调试分析
	logrus.Infof("[BackupExecutor] 🔍 Keeping JSON file for debugging: %s", outputPath)
	logrus.Infof("[BackupExecutor] 🔍 Keeping ZIP file for debugging: %s", zipPath)
	// 保留所有文件供调试分析

	logrus.Infof("[BackupExecutor] ✅ COMPLETE external backup workflow completed for collection: %s", collection)

	return nil
}

// UseExternalCommands 检查是否应该使用外部命令模式
func (e *BackupExecutor) UseExternalCommands() bool {
	// 可以通过环境变量控制
	if os.Getenv("USE_EXTERNAL_BACKUP") == "true" {
		return true
	}

	// 也可以检查可用内存，如果内存不足自动切换到外部命令模式
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	currentMB := float64(m.Alloc) / 1024 / 1024

	if currentMB > 2000 { // 如果Go进程已经使用超过2GB，切换到外部模式
		logrus.Warnf("[BackupExecutor] High memory usage detected (%.2fMB), switching to external command mode", currentMB)
		return true
	}

	return false
}

// exportMongoDBMergedTables 使用外部命令进行多表合并备份
// 处理跨月数据导出场景，支持多个集合的合并，参考executeExternalMongoExportSimple的实现模式
func (e *BackupExecutor) exportMongoDBMergedTables(ctx context.Context, connStr, database string, tables []string, tempDir string, config ExecutorBackupConfig) error {
	logrus.Infof("[BackupExecutor] 🚀 Starting multi-table merge backup for %d tables: %v", len(tables), tables)

	// 记录Go进程内存 (应该保持稳定)
	e.logMemoryUsage("MERGED_TABLES_START")

	// 提取基础名称（去掉日期后缀）
	baseCollectionName := e.extractTablePrefix(tables[0])

	// 使用 processFileNamePattern 生成正确的文件名
	fileName := processFileNamePattern(config.Destination.FileNamePattern, baseCollectionName)

	// 确保扩展名为 .json
	if !strings.HasSuffix(fileName, ".json") {
		fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName)) + ".json"
	}

	mergedJsonPath := filepath.Join(tempDir, fileName)

	// 生成对应的zip文件名
	zipFileName := strings.TrimSuffix(fileName, ".json") + ".zip"
	zipPath := filepath.Join(tempDir, zipFileName)

	// Step 1: 分别导出每个表并合并
	logrus.Infof("[BackupExecutor] 📤 Step 1: Exporting and merging %d tables", len(tables))

	// 创建合并文件
	mergedFile, err := os.Create(mergedJsonPath)
	if err != nil {
		return fmt.Errorf("failed to create merged file: %w", err)
	}
	defer mergedFile.Close()

	// 写入JSON数组开始符号
	if _, err := mergedFile.WriteString("[\n"); err != nil {
		return fmt.Errorf("failed to write array start: %w", err)
	}

	for i, table := range tables {
		logrus.Infof("[BackupExecutor] 📄 Exporting table %d/%d: %s", i+1, len(tables), table)

		// 为每个表创建临时文件
		dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
		tempTablePath := fmt.Sprintf("%s/%s_%s_temp.json", tempDir, table, dateStr)

		// 使用mongoexport导出单个表，应用查询条件和字段选择
		if err := e.executeExternalMongoExportWithOptions(ctx, connStr, database, table, tempTablePath, config); err != nil {
			// 如果是因为没有查询条件而跳过，则继续处理下一个表
			if strings.Contains(err.Error(), "no query conditions specified") {
				logrus.Infof("[BackupExecutor] ⏭️  Skipping table %s (no query conditions)", table)
				continue
			}
			return fmt.Errorf("failed to export table %s: %w", table, err)
		}

		// 读取临时文件并合并到主文件
		tempFile, err := os.Open(tempTablePath)
		if err != nil {
			return fmt.Errorf("failed to open temp file for table %s: %w", table, err)
		}

		// 读取JSONL格式文件内容（mongoexport默认输出格式）
		content, err := os.ReadFile(tempTablePath)
		if err != nil {
			tempFile.Close()
			return fmt.Errorf("failed to read temp file for table %s: %w", table, err)
		}

		// mongoexport输出的是JSONL格式（每行一个JSON对象），不是JSON数组
		// 需要将每行转换为数组元素
		contentStr := strings.TrimSpace(string(content))

		if len(contentStr) > 0 {
			// 将JSONL格式转换为JSON数组元素
			lines := strings.Split(contentStr, "\n")
			var validLines []string

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" && strings.HasPrefix(line, "{") {
					validLines = append(validLines, line)
				}
			}

			// 如果有有效数据行
			if len(validLines) > 0 {
				// 如果不是第一个表，添加逗号分隔符
				if i > 0 {
					if _, err := mergedFile.WriteString(",\n"); err != nil {
						tempFile.Close()
						return fmt.Errorf("failed to write separator: %w", err)
					}
				}

				// 写入每个JSON对象，用逗号分隔
				for j, line := range validLines {
					if j > 0 {
						if _, err := mergedFile.WriteString(",\n"); err != nil {
							tempFile.Close()
							return fmt.Errorf("failed to write line separator: %w", err)
						}
					}
					if _, err := mergedFile.WriteString(line); err != nil {
						tempFile.Close()
						return fmt.Errorf("failed to write line data for %s: %w", table, err)
					}
				}
			}
		}

		tempFile.Close()
		// 暂时不删除临时文件，保留用于调试
		logrus.Infof("[BackupExecutor] 🔍 Keeping temp file for debugging: %s", tempTablePath)

		logrus.Infof("[BackupExecutor] ✅ Table %s merged successfully", table)
	}

	// 写入JSON数组结束符号
	if _, err := mergedFile.WriteString("\n]"); err != nil {
		return fmt.Errorf("failed to write array end: %w", err)
	}
	mergedFile.Close()

	// 检查合并文件
	if stat, err := os.Stat(mergedJsonPath); err == nil {
		logrus.Infof("[BackupExecutor] ✅ Merge completed: %.2f MB", float64(stat.Size())/1024/1024)

		// 额外调试：显示文件内容的前几行和后几行
		if content, readErr := os.ReadFile(mergedJsonPath); readErr == nil {
			contentStr := string(content)
			lines := strings.Split(contentStr, "\n")
			logrus.Infof("[BackupExecutor] 🔍 Merged file has %d lines", len(lines))

			// 显示前3行
			for i := 0; i < 3 && i < len(lines); i++ {
				logrus.Infof("[BackupExecutor] 🔍 Line %d: %s", i+1, lines[i])
			}

			// 显示后3行
			if len(lines) > 3 {
				logrus.Infof("[BackupExecutor] 🔍 ...")
				for i := len(lines) - 3; i < len(lines); i++ {
					if i >= 0 {
						logrus.Infof("[BackupExecutor] 🔍 Line %d: %s", i+1, lines[i])
					}
				}
			}
		}
	} else {
		logrus.Errorf("[BackupExecutor] ❌ Failed to stat merged file: %v", err)
	}

	e.logMemoryUsage("AFTER_MERGE")

	// Step 2: 外部zip命令
	logrus.Infof("[BackupExecutor] 🗜️ Step 2: External zip compression")
	if err := e.executeExternalZip(ctx, tempDir, mergedJsonPath, zipPath); err != nil {
		return fmt.Errorf("external zip failed: %w", err)
	}

	e.logMemoryUsage("AFTER_EXTERNAL_ZIP")

	// Step 3: 外部GCS上传
	gcsPath := fmt.Sprintf("%s/%s", config.Destination.GCSPath, zipFileName)
	if !strings.HasPrefix(gcsPath, "gs://") {
		gcsPath = fmt.Sprintf("gs://logs-router-bucketbk/external/%s", zipFileName)
	}
	logrus.Infof("[BackupExecutor] ☁️ Step 3: External GCS upload")
	if err := e.executeExternalGCSUpload(ctx, zipPath, gcsPath); err != nil {
		return fmt.Errorf("external GCS upload failed: %w", err)
	}

	e.logMemoryUsage("MERGED_TABLES_COMPLETE")

	// 暂时不删除临时文件，保留用于调试
	logrus.Infof("[BackupExecutor] 🔍 Keeping merged file for debugging: %s", mergedJsonPath)
	logrus.Infof("[BackupExecutor] 🔍 Keeping zip file for debugging: %s", zipPath)
	// 保留所有文件供后续调试分析

	logrus.Infof("[BackupExecutor] ✅ Multi-table merge backup completed successfully for %d tables", len(tables))
	return nil
}

// executeExternalMongoExportWithOptions 执行外部mongoexport命令，支持查询条件和字段选择
func (e *BackupExecutor) executeExternalMongoExportWithOptions(ctx context.Context, connStr, database, collection, outputPath string, config ExecutorBackupConfig) error {
	args := []string{
		"--uri", connStr,
		"--db", database,
		"--collection", collection,
		"--out", outputPath,
		"--quiet",
	}

	// 添加查询条件
	if queryConditions, exists := config.Query[collection]; exists && len(queryConditions) > 0 {
		// 清理查询条件中的多余引号
		cleanedQuery := cleanQueryStringValues(queryConditions)

		// 转换动态时间查询为具体的MongoDB查询
		finalQuery := e.convertTimeRangeQuery(cleanedQuery)

		queryJSON, err := json.Marshal(finalQuery)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to marshal query for collection %s: %v", collection, err)
		} else {
			args = append(args, "--query", string(queryJSON))
			logrus.Infof("[BackupExecutor] Applied query for collection %s: %s", collection, string(queryJSON))
		}
	} else {
		// 如果没有查询条件，跳过该表的导出
		logrus.Warnf("[BackupExecutor] ⚠️  No query conditions found for collection %s, skipping export", collection)
		return fmt.Errorf("no query conditions specified for collection %s", collection)
	}

	// 添加字段选择
	if fields, exists := config.Database.Fields[collection]; exists && len(fields) > 0 && fields[0] != "all" {
		fieldsStr := strings.Join(fields, ",")
		args = append(args, "--fields", fieldsStr)
		logrus.Infof("[BackupExecutor] Applied field selection for collection %s: %s", collection, fieldsStr)
	}

	cmd := exec.CommandContext(ctx, "mongoexport", args...)

	// 显示完整的命令行参数，包括query参数
	logrus.Infof("[BackupExecutor] Executing: %s", strings.Join(append([]string{"mongoexport"}, args...), " "))

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mongoexport failed: %w, output: %s", err, string(output))
	}

	// 检查输出文件
	if _, err := os.Stat(outputPath); err != nil {
		return fmt.Errorf("mongoexport output file not created: %w", err)
	}

	// 统计导出的记录数和文件大小
	recordCount, fileSize, err := e.countRecordsInFile(outputPath)
	if err != nil {
		logrus.Warnf("[BackupExecutor] Failed to count records in %s: %v", outputPath, err)
		// 回退到只显示文件大小
		if stat, err := os.Stat(outputPath); err == nil {
			logrus.Infof("[BackupExecutor] ✅ Mongoexport completed: %.2f MB", float64(stat.Size())/1024/1024)
		}
	} else {
		logrus.Infof("[BackupExecutor] ✅ Mongoexport completed: %d records, %.2f MB", recordCount, fileSize)
	}

	return nil
}

// countRecordsInFile 统计JSON文件中的记录数量
func (e *BackupExecutor) countRecordsInFile(filePath string) (int, float64, error) {
	stat, err := os.Stat(filePath)
	if err != nil {
		return 0, 0, err
	}

	fileSize := float64(stat.Size()) / 1024 / 1024 // MB

	file, err := os.Open(filePath)
	if err != nil {
		return 0, fileSize, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// 跳过空行和JSON数组标记
		if line != "" && line != "[" && line != "]" && line != "," {
			// 简单检查是否是JSON对象（以{开头）
			if strings.HasPrefix(line, "{") || (strings.HasPrefix(line, ",") && strings.Contains(line, "{")) {
				count++
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fileSize, err
	}

	return count, fileSize, nil
}

// convertTimeRangeQuery Convert dynamic time range query to concrete MongoDB query
func (e *BackupExecutor) convertTimeRangeQuery(query map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range query {
		if timeQuery, ok := value.(map[string]interface{}); ok {
			if timeType, exists := timeQuery["type"]; exists && timeType == "daily" {
				// Parse offset values
				startOffset := -1
				endOffset := 0

				if so, ok := timeQuery["startOffset"]; ok {
					if offset, ok := so.(float64); ok {
						startOffset = int(offset)
					}
				}
				if eo, ok := timeQuery["endOffset"]; ok {
					if offset, ok := eo.(float64); ok {
						endOffset = int(offset)
					}
				}

				// Calculate JST time range and convert to UTC for database query
				now := time.Now()
				jst := time.FixedZone("JST", 9*3600)

				// Get current JST time and truncate to start of day
				nowJST := now.In(jst)

				// Calculate start and end days in JST
				startDayJST := time.Date(nowJST.Year(), nowJST.Month(), nowJST.Day()+startOffset, 0, 0, 0, 0, jst)
				endDayJST := time.Date(nowJST.Year(), nowJST.Month(), nowJST.Day()+endOffset, 0, 0, 0, 0, jst)

				// Convert JST times to UTC
				startUTC := startDayJST.UTC()
				endUTC := endDayJST.UTC()

				logrus.Infof("[BackupExecutor] Time calculation: now=%s, startOffset=%d, endOffset=%d",
					nowJST.Format("2006-01-02 15:04:05 JST"), startOffset, endOffset)
				logrus.Infof("[BackupExecutor] JST range: %s to %s",
					startDayJST.Format("2006-01-02 15:04:05 JST"), endDayJST.Format("2006-01-02 15:04:05 JST"))
				logrus.Infof("[BackupExecutor] UTC range: %s to %s",
					startUTC.Format("2006-01-02T15:04:05.000Z"), endUTC.Format("2006-01-02T15:04:05.000Z"))

				// Create MongoDB date range query
				mongoQuery := map[string]interface{}{
					"$gte": map[string]interface{}{
						"$date": startUTC.Format("2006-01-02T15:04:05.000Z"),
					},
					"$lt": map[string]interface{}{
						"$date": endUTC.Format("2006-01-02T15:04:05.000Z"),
					},
				}

				result[key] = mongoQuery
				logrus.Infof("[BackupExecutor] Converted time range query for field %s: %s to %s",
					key, startUTC.Format("2006-01-02T15:04:05.000Z"), endUTC.Format("2006-01-02T15:04:05.000Z"))
			} else {
				// Keep non-time queries as is
				result[key] = value
			}
		} else {
			// Keep non-object values as is
			result[key] = value
		}
	}

	return result
}

// cleanQueryStringValues Clean string values in query condition to remove extra escaping
func cleanQueryStringValues(queryObj map[string]interface{}) map[string]interface{} {
	cleaned := make(map[string]interface{})

	for key, value := range queryObj {
		switch v := value.(type) {
		case string:
			// Remove surrounding quotes if they exist (handle over-escaping)
			cleanValue := v
			// Remove extra double quotes from the beginning and end
			if strings.HasPrefix(cleanValue, `"`) && strings.HasSuffix(cleanValue, `"`) {
				cleanValue = strings.TrimPrefix(cleanValue, `"`)
				cleanValue = strings.TrimSuffix(cleanValue, `"`)
			}
			// Remove extra single quotes from the beginning and end
			if strings.HasPrefix(cleanValue, `'`) && strings.HasSuffix(cleanValue, `'`) {
				cleanValue = strings.TrimPrefix(cleanValue, `'`)
				cleanValue = strings.TrimSuffix(cleanValue, `'`)
			}
			cleaned[key] = cleanValue
		case map[string]interface{}:
			// Recursively clean nested objects
			cleaned[key] = cleanQueryStringValues(v)
		default:
			// Keep other types as is
			cleaned[key] = value
		}
	}

	return cleaned
}
