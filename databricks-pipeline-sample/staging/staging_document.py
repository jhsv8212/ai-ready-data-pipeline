"""Staging Layer: S3 Landing Zone 파일 도착 이벤트를 기록합니다.

입력: S3 Landing Zone (Auto Loader cloudFiles)
출력: staging_documents (Streaming Table)
  - S3 Landing Zone에 파일이 도착하면 Auto Loader가 감지하여 메타데이터를 기록합니다.
  - allowOverwrites=true로 동일 파일이 재업로드(수정)될 때마다 새 행을 append합니다.
  - 하위 레이어(Bronze, Silver, Gold)의 모든 스트리밍 테이블이 이 테이블을 소스로 사용합니다.
"""
import sys
sys.path.insert(0, "/Workspace/Shared/rag_document_processing_pipeline_f237c77c/transformations")

from pyspark import pipelines as dp
from pyspark.sql import functions as F

import config


@dp.table(
    name="staging.staging_documents",
    comment="S3 Landing Zone 파일 도착 이벤트 원장 (Staging Layer) - Auto Loader로 파일 메타데이터 수집",
    schema="""
        source_file STRING,
        source_file_name STRING,
        file_size_bytes BIGINT,
        file_modified_at TIMESTAMP,
        ingested_at TIMESTAMP
    """,
)
def staging_documents():
    s3_landing_path = spark.conf.get(
        "s3_landing_path",
        config.S3_LANDING_PATH_DEFAULT,
    )

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.allowOverwrites", "true")
        .option("recursiveFileLookup", "true")
        .load(s3_landing_path)
        .select(
            F.col("path").alias("source_file"),
            F.element_at(F.split(F.col("path"), "/"), -1).alias("source_file_name"),
            F.col("length").alias("file_size_bytes"),
            F.col("modificationTime").alias("file_modified_at"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
