"""Bronze Layer: S3 Landing Zone의 PDF 파일을 Auto Loader로 수집합니다.

입력: s3_landing_path (pipeline configuration)
출력: bronze_documents (Streaming Table)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

import config


@dp.table(comment="S3 Landing Zone에서 PDF 파일을 수집한 원시 바이너리 데이터 (Bronze Layer)")
def bronze_documents():
    # 파이프라인 configuration에서 S3 경로를 읽음 (기본값: 고정 경로)
    s3_landing_path = spark.conf.get(
        "s3_landing_path",
        "s3://databricks-storage-7474657118263619/unity-catalog/7474657118263619/landing/documents/",
    )

    return (
        # Auto Loader: binaryFile 포맷으로 S3 PDF를 스트리밍 수집
        # 새 파일이 추가될 때마다 자동으로 감지하여 처리 (exactly-once 보장)
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .load(s3_landing_path)
        # 파일 경로에서 파일명 추출 (마지막 '/' 이후 문자열)
        .withColumn("source_file_name", F.element_at(F.split(F.col("path"), "/"), -1))
        # 확장자 제거하여 document_id 생성 (예: '약관.pdf' → '약관')
        .withColumn("document_id", F.regexp_replace(F.col("source_file_name"), r"\.[^.]+$", ""))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("bronze_layer", F.lit("bronze"))
        .select(
            "document_id",        # 문서 식별자 (파일명에서 확장자 제거)
            "source_file_name",   # 원본 파일명 (예: 약관.pdf)
            F.col("path").alias("source_file"),           # 전체 S3 경로
            F.col("content"),                             # PDF 바이너리 (Silver에서 ai_parse_document로 파싱)
            F.col("length").alias("file_size_bytes"),     # 파일 크기 (bytes)
            F.col("modificationTime").alias("file_modified_at"),  # S3 파일 수정 시각
            "ingested_at",        # 파이프라인 수집 시각
            "bronze_layer",       # 레이어 식별자
        )
    )
