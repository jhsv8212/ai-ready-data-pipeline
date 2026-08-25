sequenceDiagram
    autonumber
    participant S3 as S3 Landing Zone
    participant Job as Lakeflow Job<br/>(File Arrival Trigger)
    participant Staging as staging_documents<br/>(전 카테고리 공통)
    participant Bronze as {category}_bronze_documents
    participant Silver as {category}_silver_documents
    participant LLM as Model Serving<br/>(ai_query)
    participant Chunks as {category}_silver_document_chunks
    participant Summary as {category}_gold_document_ai_summary
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
    Silver->>Silver: full_text = content.cast(STRING)<br/>(ai_parse_document 미사용 - PDF 로직은 주석 처리로 보존)<br/>figure_descriptions/page_count/parsed_content는 항상 NULL

    par Gold - AI 요약 / 키워드 / 메타데이터
        Silver->>Summary: {category}_silver_documents 스트림 read
        Summary->>LLM: ai_query() 요약 생성
        Summary->>LLM: ai_query() 키워드 추출
        Summary->>LLM: ai_query() 메타데이터(JSON) 추출
        LLM-->>Summary: summary, keywords, metadata
    and Silver/Gold - 청킹 및 임베딩 소스
        Silver->>Chunks: {category}_silver_documents 스트림 read
        Chunks->>Chunks: full_text 전체를 단일 요소로 오버랩 청킹<br/>(overlap_chunk UDF, AI 미사용 / CHUNK_SIZE=500, OVERLAP=100)
        Chunks->>Embeddings: {category}_silver_document_chunks 스트림 read
        Embeddings->>Embeddings: chunk_id = md5(document_id, element_idx, chunk_idx)
    end

    Embeddings->>VS: Delta Sync (CDF 기반, 카테고리별 인덱스)
    VS->>VS: chunk_content 자동 임베딩<br/>(databricks-qwen3-embedding-0-6b)

    Note over VS: RAG 애플리케이션에서<br/>similarity_search() 호출
