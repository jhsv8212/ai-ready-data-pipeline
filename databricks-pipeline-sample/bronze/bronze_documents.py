"""Bronze Layer: Staging의 파일 메타데이터를 기반으로 S3에서 바이너리 콘텐츠를 수집합니다.

입력: staging_documents (Streaming Table) + S3 binary files (static)
출력: {상품명}_bronze_documents (Streaming Table, 상품별로 동적 생성)
  - config.get_product_list()로 S3 Landing Zone 1뎁스 폴더(상품명)를 스캔하고,
    상품마다 별도 테이블(예: 종신보험_bronze_documents)을 생성한다.
  - 각 테이블은 해당 상품 폴더(s3_landing_path/{상품명}/) 아래 파일만 포함한다.

TODO: 현재는 매 트리거마다 S3의 "현재" 바이너리를 static join으로 읽어오므로,
staging_document_versions에 기록된 과거 버전의 실제 파일 내용(바이너리)은 보존되지 않는다.
S3 버킷 버저닝(콘솔에서 설정 완료)의 VersionId와 연동해 과거 버전 콘텐츠까지 조회/대조하는
기능은 이번 범위에서 제외 — 필요 시 별도 작업으로 진행.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

import config


def _generate_bronze_documents(product: str):
    """상품 하나에 대한 {product}_bronze_documents 테이블을 정의한다."""

    @dp.table(
        name=f"dev_haesung.bronze.{product}_bronze_documents",
        comment=f"S3 Landing Zone에서 '{product}' 상품 문서 파일(MD)을 수집한 원시 바이너리 데이터 (Bronze Layer)",
        schema=f"""
            document_id STRING NOT NULL,
            source_file_name STRING,
            source_file STRING,
            content BINARY,
            file_size_bytes BIGINT,
            file_modified_at TIMESTAMP,
            ingested_at TIMESTAMP,
            bronze_layer STRING,
            CONSTRAINT `pk_{product}_bronze_documents` PRIMARY KEY (document_id)
        """,
    )
    def bronze_documents():
        s3_landing_path = spark.conf.get(
            "s3_landing_path",
            config.S3_LANDING_PATH_DEFAULT,
        )
        product_path = f"{s3_landing_path}{product}/"

        # Staging에서 이 상품 폴더에 속한 파일 메타데이터만 스트리밍 읽기
        staging_stream = (
            spark.readStream.table("dev_haesung.staging.staging_documents")
            .filter(F.col("source_file").startswith(product_path))
        )

        # S3에서 이 상품 폴더의 바이너리 콘텐츠 static 읽기 (각 트리거마다 최신 상태 반영)
        binary_files = (
            spark.read.format("binaryFile")
            .option("recursiveFileLookup", "true")
            .load(product_path)
        )

        # Stream-static join: staging 메타데이터 + S3 바이너리
        return (
            staging_stream
            .join(
                binary_files,
                staging_stream.source_file == binary_files.path,
                "inner",
            )
            # document_id 생성 규칙: source_file_name에서 마지막 확장자만 제거한 값을 그대로 사용
            # (예: "agreement_v1.md" -> "agreement_v1"). 경로(source_file)는 사용하지 않으므로
            # 같은 상품 폴더 내 서로 다른 하위 폴더에 동일 파일명이 존재하거나 파일이
            # 재업로드(버전 갱신)되면 동일한 document_id가 재사용된다
            # (pk_{product}_bronze_documents PK 제약과 연동됨).
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

    return bronze_documents


for _product in config.get_product_list():
    _generate_bronze_documents(_product)
