erDiagram
    staging_documents {
        STRING source_file "S3 전체 경로 (자연키)"
        STRING source_file_name "원본 파일명"
        LONG file_size_bytes "파일 크기(바이트)"
        TIMESTAMP file_modified_at "파일 수정 시각"
        TIMESTAMP ingested_at "수집 시각"
        INT version_number "(버전 조회 시) source_file별 도착 순번 - row_number()"
        INT total_versions "(버전 조회 시) source_file별 총 버전 수"
        BOOLEAN is_latest_version "(버전 조회 시) 최신 버전 여부"
    }

    category_bronze_documents {
        STRING document_id PK "문서 고유 식별자 (source_file_name 확장자 제거)"
        STRING source_file_name "원본 파일명"
        STRING source_file "S3 전체 경로"
        BINARY content "MD 바이너리 데이터"
        LONG file_size_bytes "파일 크기(바이트)"
        TIMESTAMP file_modified_at "파일 수정 시각"
        TIMESTAMP ingested_at "수집 시각"
        STRING bronze_layer "레이어 식별자"
    }

    category_silver_documents {
        STRING document_id PK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING source_file "S3 전체 경로"
        STRING full_text "MD 원문 텍스트 (content를 STRING으로 캐스팅)"
        STRING figure_descriptions "항상 NULL (PDF 전용, MD 전환으로 비활성)"
        INT page_count "항상 NULL (PDF 전용, MD 전환으로 비활성)"
        VARIANT parsed_content "항상 NULL (ai_parse_document 미사용)"
        LONG file_size_bytes "파일 크기(바이트)"
        TIMESTAMP file_modified_at "파일 수정 시각"
        TIMESTAMP ingested_at "수집 시각"
        TIMESTAMP processed_at "처리 완료 시각"
        STRING silver_layer "레이어 식별자"
    }

    category_silver_document_chunks {
        STRING document_id FK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING element_type "항상 'text' (MD 전환 - PDF 복귀 시 table/figure 등 다양화)"
        INT element_page "항상 NULL (MD 전환으로 페이지 개념 없음)"
        INT element_idx "항상 0 (문서 전체를 단일 요소로 취급)"
        INT chunk_idx "요소 내 청크 인덱스"
        STRING chunk_content "청크 텍스트 내용"
        STRING chunk_type "청크 유형(single/overlap)"
        TIMESTAMP chunked_at "청킹 처리 시각"
    }

    category_gold_document_ai_summary {
        STRING document_id FK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING extraction_method "추출 방식 (예: LLM 추출 - 청크 본문 기반)"
        STRING summary "AI 생성 문서 요약"
        ARRAY_STRING keywords "AI 추출 키워드 태그 목록"
        VARIANT metadata "JSON 형태 메타데이터 (아래 예시 참조)"
        TIMESTAMP generated_at "요약 생성 시각"
    }

    category_gold_document_embeddings {
        STRING chunk_id PK "청크 고유 식별자"
        STRING document_id FK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING element_type "요소 타입"
        INT element_page "요소가 위치한 페이지"
        INT element_idx "페이지 내 요소 인덱스"
        INT chunk_idx "요소 내 청크 인덱스"
        STRING chunk_content "청크 텍스트 내용"
        STRING chunk_type "청크 유형"
        TIMESTAMP chunked_at "청킹 처리 시각"
    }

    category_gold_document_embeddings_index {
        STRING chunk_id PK "청크 고유 식별자"
        STRING document_id FK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING element_type "요소 타입"
        INT element_page "요소가 위치한 페이지"
        INT element_idx "페이지 내 요소 인덱스"
        STRING chunk_content "청크 텍스트 내용"
        STRING chunk_type "청크 유형"
        TIMESTAMP chunked_at "청킹 처리 시각"
        ARRAY_FLOAT __db_chunk_content_vector "임베딩 벡터(qwen3-embedding-0-6b, 1024d)"
    }

    staging_documents ||--|| category_bronze_documents : "1:1 source_file join (category_path 필터)"
    category_bronze_documents ||--|| category_silver_documents : "1:1 텍스트 디코딩 (content.cast(STRING))"
    category_silver_documents ||--o{ category_silver_document_chunks : "1:N Chunk (문서 전체 오버랩 청킹)"
    category_silver_documents ||--|| category_gold_document_ai_summary : "1:1 AI Summary"
    category_silver_document_chunks ||--|| category_gold_document_embeddings : "1:1 chunk_id 할당"
    category_gold_document_embeddings ||--|| category_gold_document_embeddings_index : "1:1 Vector Sync (Delta Sync, 카테고리별 인덱스)"