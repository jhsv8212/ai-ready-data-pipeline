"""Bronze Layer: Staging의 파일 메타데이터를 기반으로 S3에서 바이너리 콘텐츠를 수집합니다.

입력: staging_documents (Streaming Table) + S3 binary files (static)
출력: bronze_documents (Streaming Table)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

import config


@dp.table(
    name="dev_haesung.bronze.bronze_documents",
    comment="S3 Landing Zone에서 PDF 파일을 수집한 원시 바이너리 데이터 (Bronze Layer)",
    schema="""
        document_id STRING NOT NULL,
        source_file_name STRING,
        source_file STRING,
        content BINARY,
        file_size_bytes BIGINT,
        file_modified_at TIMESTAMP,
        ingested_at TIMESTAMP,
        bronze_layer STRING,
        CONSTRAINT pk_bronze_documents PRIMARY KEY (document_id)
    """,
)
def bronze_documents():
    s3_landing_path = spark.conf.get(
        "s3_landing_path",
        config.S3_LANDING_PATH_DEFAULT,
    )

    # Staging에서 파일 메타데이터 스트리밍 읽기
    staging_stream = spark.readStream.table("dev_haesung.staging.staging_documents")

    # S3에서 바이너리 콘텐츠 static 읽기 (각 트리거마다 최신 상태 반영)
    binary_files = (
        spark.read.format("binaryFile")
        .load(s3_landing_path)
    )

    # Stream-static join: staging 메타데이터 + S3 바이너리
    return (
        staging_stream
        .join(
            binary_files,
            staging_stream.source_file == binary_files.path,
            "inner",
        )
        .withColumn(
            "document_id",
            F.regexp_replace(F.col("source_file_name"), r"\.[^.]+$", ""),
        )
        .withColumn("bronze_layer", F.lit("bronze"))
        .select(
            "document_id",
            "source_file_name",
            staging_stream.source_file,
            binary_files.content,
            staging_stream.file_size_bytes,
            staging_stream.file_modified_at,
            staging_stream.ingested_at,
            "bronze_layer",
        )
    )
