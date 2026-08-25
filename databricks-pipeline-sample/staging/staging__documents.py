"""Staging Layer: S3 Landing Zone의 파일 메타데이터를 추적합니다.

입력: s3_landing_path (pipeline configuration)
출력: staging_documents (Streaming Table)
  - 파일 도착 이력 및 버전 관리
  - allowOverwrites=true로 파일 재업로드 감지
  - 바이너리 content는 저장하지 않음 (메타데이터만)
  - category_name: s3_landing_path(보험/) 바로 아래 1뎁스 폴더명(카테고리명) 추출.
    이 컬럼을 기준으로 bronze/silver/gold 레이어가 카테고리별 테이블로 분기됩니다.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

import config


@dp.table(
    name="dev_haesung.staging.staging_documents",
    comment="S3 Landing Zone 파일 메타데이터 및 버전 이력 (Staging Layer)",
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
        .withColumn(
            "source_file_name",
            F.element_at(F.split(F.col("path"), "/"), -1),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .select(
            F.col("path").alias("source_file"),
            "source_file_name",
            F.col("length").alias("file_size_bytes"),
            F.col("modificationTime").alias("file_modified_at"),
            "ingested_at",
        )
    )
