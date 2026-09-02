sequenceDiagram
    autonumber
    participant S3 as S3 Landing Zone
    participant Job as Lakeflow Job<br/>(File Arrival Trigger)
    participant Staging as staging_documents<br/>(전 카테고리 공통)
    participant Bronze as {category}_bronze_documents
    participant Silver as {category}_silver_documents
    participant Chunks as {category}_silver_document_chunks
    participant Embeddings as {category}_gold_document_embeddings
    participant VS as Vector Search Index

    Note over S3: MD 업로드
    S3->>Job: 파일 도착 감지
    Note over Job: 마지막 파일 변경 후 300초 대기<br/>최소 실행 간격 600초
    Job->>Staging: 파이프라인 업데이트 시작
    Note over Job: 그래프 빌드 시점에 config.get_category_list()로<br/>S3 1뎁스 폴더를 스캔해 카테고리 목록 확보 →<br/>bronze/silver/gold 각 .py의 for 루프가 카테고리별 테이블 생성

    Staging->>S3: Auto Loader (cloudFiles, binaryFile,<br/>allowOverwrites=true, recursiveFileLookup=true)
    Staging->>Staging: 전 카테고리 파일 메타데이터 수집<br/>(append-only 이벤트 원장, 바이너리 미저장)
    Note over Staging: staging_documents 전체 read → source_file별<br/>row_number()로 version_number / is_latest_version 계산<br/>(내부적으로 staging_document_versions Materialized View로 구현)

    Staging->>Bronze: staging_documents 스트림 read<br/>+ source_file.startswith(category_path) 필터
    Bronze->>S3: binaryFile static read<br/>(category_path만 recursiveFileLookup=true)
    Bronze->>Bronze: stream-static join<br/>(source_file = path)<br/>document_id = source_file_name 확장자 제거

    Bronze->>Silver: {category}_bronze_documents 스트림 read
    Silver->>Silver: full_text = content.cast(STRING)<br/>(ai_parse_document 미사용 - PDF 로직은 주석 처리로 보존)

    Silver->>Chunks: {category}_silver_documents 스트림 read
    Chunks->>Chunks: full_text 전체를 단일 요소로 오버랩 청킹<br/>(overlap_chunk UDF, AI 미사용 / CHUNK_SIZE=1000, OVERLAP=200)

    Chunks->>Embeddings: {category}_silver_document_chunks 스트림 read
    Embeddings->>Embeddings: chunk_id = md5(document_id, element_idx, chunk_idx)
    Note over Embeddings: embed_with_bge_m3_api pandas_udf가 chunk_content를<br/>bge-m3 FastAPI 서비스로 배치 전송 → embedding(1024d) 저장<br/>+ source_file 경로로 doc_path/agent_id 계산 → metadata(JSON) 저장

    Job->>VS: 파이프라인 완료 후 setup_vector_search_index.py Job 태스크 실행
    VS->>Embeddings: Delta Sync (카테고리별 인덱스)
    Note over VS: ⚠ 현재 Job 태스크는 embedding_source_columns 방식(chunk_content를<br/>databricks-qwen3-embedding-0-6b로 자동 계산)을 사용 중이며,<br/>파이프라인이 이미 저장한 embedding 컬럼과는 별개 계산 방식 - 정리 필요<br/>(embedding_vector_column 방식으로 통일하려면 copy_gold_and_sync_indexes.py 참고)
