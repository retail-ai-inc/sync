#!/bin/bash

# External Command-Based Backup Script
# 避免Go内存管理问题，使用外部命令处理大文件操作

set -e

# 配置参数
MONGO_URI="$1"
DATABASE="$2" 
COLLECTION="$3"
OUTPUT_DIR="$4"
GCS_BUCKET="$5"
DATE_STR="${6:-$(date +%Y-%m-%d)}"

# 验证参数
if [ $# -lt 5 ]; then
    echo "Usage: $0 <mongo_uri> <database> <collection> <output_dir> <gcs_bucket> [date_str]"
    echo "Example: $0 'mongodb://user:pass@host:27017' mydb mycoll /tmp gs://my-bucket"
    exit 1
fi

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# 内存监控函数
monitor_memory() {
    local phase="$1"
    local mem_total=$(grep MemTotal /proc/meminfo | awk '{print int($2/1024)}')
    local mem_free=$(grep MemFree /proc/meminfo | awk '{print int($2/1024)}')
    local mem_avail=$(grep MemAvailable /proc/meminfo | awk '{print int($2/1024)}')
    log "📊 Memory [$phase]: Total=${mem_total}MB, Free=${mem_free}MB, Available=${mem_avail}MB"
}

# 错误处理函数
cleanup_on_error() {
    local exit_code=$?
    log "❌ Error occurred (exit code: $exit_code), cleaning up..."
    
    # 清理临时文件
    [ -f "$JSON_FILE" ] && rm -f "$JSON_FILE"
    [ -f "$ZIP_FILE" ] && rm -f "$ZIP_FILE"
    
    # 杀死可能还在运行的后台进程
    pkill -f "mongoexport.*$COLLECTION" 2>/dev/null || true
    pkill -f "zip.*$COLLECTION" 2>/dev/null || true
    pkill -f "gsutil.*$COLLECTION" 2>/dev/null || true
    
    exit $exit_code
}

trap cleanup_on_error ERR INT TERM

# 文件路径设置
JSON_FILE="${OUTPUT_DIR}/${COLLECTION}_${DATE_STR}.json"
ZIP_FILE="${OUTPUT_DIR}/${COLLECTION}_${DATE_STR}.zip"
GCS_PATH="gs://${GCS_BUCKET}/${COLLECTION}_${DATE_STR}.zip"

log "🚀 Starting external command-based backup"
log "📋 Config: DB=$DATABASE, Collection=$COLLECTION, Output=$OUTPUT_DIR"

# 确保输出目录存在
mkdir -p "$OUTPUT_DIR"

monitor_memory "START"

# Step 1: MongoDB Export with streaming to avoid memory buildup
log "📤 Step 1: MongoDB Export (streaming mode)"

# 获取文档数量
TOTAL_DOCS=$(mongo "$MONGO_URI" --quiet --eval "db.getSiblingDB('$DATABASE').$COLLECTION.countDocuments({})" 2>/dev/null || echo "0")
log "📊 Total documents: $TOTAL_DOCS"

# 直接一次性导出 - mongoexport会自己管理内存
log "📄 Direct mongoexport (single export)"
mongoexport \
    --uri="$MONGO_URI" \
    --db="$DATABASE" \
    --collection="$COLLECTION" \
    --out="$JSON_FILE" \
    --quiet

# 检查导出文件
if [ ! -f "$JSON_FILE" ]; then
    log "❌ Export failed: JSON file not created"
    exit 1
fi

EXPORT_SIZE=$(stat -f%z "$JSON_FILE" 2>/dev/null || stat -c%s "$JSON_FILE" 2>/dev/null || echo "0")
EXPORT_SIZE_MB=$((EXPORT_SIZE / 1024 / 1024))
log "✅ Export completed: ${EXPORT_SIZE_MB}MB"

monitor_memory "EXPORT_COMPLETE"

# Step 2: ZIP Compression (external zip process)
log "🗜️ Step 2: ZIP Compression (external process)"

# 使用系统zip命令，避免Go内存管理
cd "$OUTPUT_DIR"
zip -q "$ZIP_FILE" "$(basename "$JSON_FILE")"

# 检查压缩文件
if [ ! -f "$ZIP_FILE" ]; then
    log "❌ Compression failed: ZIP file not created"
    exit 1
fi

ZIP_SIZE=$(stat -f%z "$ZIP_FILE" 2>/dev/null || stat -c%s "$ZIP_FILE" 2>/dev/null || echo "0")
ZIP_SIZE_MB=$((ZIP_SIZE / 1024 / 1024))
COMPRESSION_RATIO=$(( (EXPORT_SIZE - ZIP_SIZE) * 100 / EXPORT_SIZE ))

log "✅ Compression completed: ${ZIP_SIZE_MB}MB (${COMPRESSION_RATIO}% reduction)"

monitor_memory "COMPRESSION_COMPLETE"

# Step 3: GCS Upload (external gsutil process)
log "☁️ Step 3: GCS Upload (external process)"

# 使用gsutil的流式上传，避免内存缓存整个文件
gsutil -o GSUtil:parallel_composite_upload_threshold=150M cp "$ZIP_FILE" "$GCS_PATH"

# 验证上传
if gsutil ls "$GCS_PATH" >/dev/null 2>&1; then
    log "✅ Upload completed: $GCS_PATH"
else
    log "❌ Upload failed: Could not verify file in GCS"
    exit 1
fi

monitor_memory "UPLOAD_COMPLETE"

# Step 4: Cleanup
log "🧹 Step 4: Cleanup"

# 删除本地临时文件
rm -f "$JSON_FILE"
rm -f "$ZIP_FILE"

log "🎉 External backup workflow completed successfully!"
log "📊 Final stats: Collection=$COLLECTION, Documents=$TOTAL_DOCS, Compressed=${ZIP_SIZE_MB}MB"

monitor_memory "WORKFLOW_COMPLETE"