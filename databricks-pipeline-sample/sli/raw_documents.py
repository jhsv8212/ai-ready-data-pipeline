from pyspark import pipelines as dp

# =============================================================================
# Bronze Layer: Raw Document Ingestion
# =============================================================================
# Auto Loader를 사용하여 외부 스토리지에서 문서 파일을 재귀적으로 읽어옵니다.
# 지원 형식: PDF, TXT, DOCX, PPTX 등 (binaryFile 형식으로 원본 바이트 인제스트)
# =============================================================================

# TODO: 아래 경로를 실제 외부 스토리지 경로로 변경하세요.
# 예시:
#   - Volume: "/Volumes/<catalog>/<schema>/<volume>/documents/"
#   - External Location: "s3://your-bucket/documents/"
#   - ADLS: "abfss://container@account.dfs.core.windows.net/documents/"
DOCUMENT_SOURCE_PATH = "s3://a-s3-dbx-dev-ane2-aegis01/보험/"


@dp.table(
    name="raw_documents",
    comment="Bronze: 외부 스토리지에서 재귀적으로 인제스트된 원본 문서 파일 (바이너리)",
)
def raw_documents():
    # 파일 확장자 필터 필요시
    # .option("pathGlobFilter", "*.{pdf,txt,docx,pptx,doc,ppt}")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("recursiveFileLookup", "true")
        .load(DOCUMENT_SOURCE_PATH)
    )
