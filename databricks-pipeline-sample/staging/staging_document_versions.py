"""Staging Layer: source_file별 재수집(재업로드) 버전 이력을 집계합니다.

입력: staging_documents (Streaming Table, 파일 도착 이벤트 원장)
출력: staging_document_versions (Materialized View)
  - staging_documents는 allowOverwrites=true로 동일 source_file이 재업로드될 때마다
    새 행을 append하는 이벤트 원장(append-only log)입니다.
  - 이 테이블은 그 원장을 source_file 기준으로 묶어 도착 순서(version_number)와
    최신 버전 여부(is_latest_version)를 계산해 히스토리 조회를 쉽게 만듭니다.

TODO: 여기서 관리하는 버전은 메타데이터(도착 이력) 기준이며, 과거 버전의 실제 파일
바이너리 콘텐츠는 저장/대조하지 않는다. S3 버킷 버저닝(콘솔에서 설정 완료)의 VersionId와
연동한 실제 콘텐츠 히스토리 조회는 이번 범위에서 제외 — 필요 시 별도 작업으로 진행.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="staging.staging_document_versions",
    comment="source_file별 재수집 버전 이력 (Staging Layer) - staging_documents 이벤트 원장을 집계",
)
def staging_document_versions():
    version_order = Window.partitionBy("source_file").orderBy(
        "file_modified_at", "ingested_at"
    )
    per_file = Window.partitionBy("source_file")

    return (
        spark.read.table("staging.staging_documents")
        .withColumn("version_number", F.row_number().over(version_order))
        .withColumn("total_versions", F.max("version_number").over(per_file))
        .withColumn(
            "is_latest_version",
            F.col("version_number") == F.col("total_versions"),
        )
        .select(
            "source_file",
            "source_file_name",
            "file_size_bytes",
            "file_modified_at",
            "ingested_at",
            "version_number",
            "total_versions",
            "is_latest_version",
        )
    )
