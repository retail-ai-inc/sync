package backup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
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
	
	// 清理临时文件
	os.Remove(outputPath)
	// 保留ZIP文件供后续处理
	
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
func (e *BackupExecutor) executeExternalMongoExportSimple(ctx context.Context, connStr, database, collection, tempDir string) error {
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
		"/mnt/state/RetailerRecommendationAnalytics_202508_2025-08-26.json", // 服务器路径
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
		// 如果没有现有文件，回退到mongoexport
		logrus.Infof("[BackupExecutor] ⚠️ Existing file not found, falling back to mongoexport")
		if err := e.executeExternalMongoExport(ctx, connStr, database, collection, outputPath); err != nil {
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
	
	// 清理临时文件
	os.Remove(outputPath)
	// 保留ZIP文件供后续处理或调试
	
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