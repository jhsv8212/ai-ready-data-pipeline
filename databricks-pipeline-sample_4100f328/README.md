# databricks-pipeline-sample

S3에 적재된 보험 문서(PDF)를 Auto Loader로 수집하고, Databricks AI 함수로 텍스트를 추출·요약하는 Lakeflow Spark Declarative Pipeline입니다.

---

## 목차

1. [아키텍처](#아키텍처)
2. [데이터셋](#데이터셋)
3. [파일 구조](#파일-구조)
4. [파이프라인 콘솔 설정 가이드](#파이프라인-콘솔-설정-가이드)
5. [대시보드 콘솔 설정 가이드](#대시보드-콘솔-설정-가이드)
6. [실행 방법](#실행-방법)
7. [자동 트리거](#자동-트리거)
8. [사용 AI 함수](#사용-ai-함수)

---

## 아키텍처

```
S3 Landing Zone (PDF)
        │
        ▼ Auto Loader (binaryFile)
┌─────────────────────┐
│   bronze_documents  │  Streaming Table — PDF 파일 그대로 수집
└─────────────────────┘
        │
        ▼ ai_parse_document()
┌─────────────────────┐
│   silver_documents  │  Materialized View — 구조화된 텍스트 + figure 설명 + VARIANT
└─────────────────────┘
        │
        ├──────────────────────────────────────────┬──────────────────────────┐
        ▼                                          ▼                          ▼
┌──────────────────────┐      ┌────────────────────────────┐  ┌──────────────────────┐
│ gold_document_summary│      │ gold_document_ai_summary   │  │ gold_category_metrics│
│ (문서 요약 테이블)    │      │ (AI 요약 - ai_query)        │  │ (일자별 집계 지표)    │
└──────────────────────┘      └────────────────────────────┘  └──────────────────────┘
```

---

## 데이터셋

| 테이블 | 타입 | 설명 |
|---|---|---|
| `bronze_documents` | Streaming Table | S3 PDF 원본 바이너리 수집 |
| `silver_documents` | Materialized View | `ai_parse_document()`로 추출한 텍스트 및 메타데이터 |
| `gold_document_summary` | Materialized View | 문서별 요약 정보 (text_preview, page_count, total_chars) |
| `gold_category_metrics` | Materialized View | 일자별 집계 지표 (문서 수, 총 페이지, 총 글자수) |
| `gold_document_ai_summary` | Materialized View | `ai_query()`로 생성한 문서별 한국어 AI 요약 |

---

## 파일 구조

```
databricks-pipeline-sample_4100f328/
├── README.md
└── pipeline/
    ├── bronze/
    │   └── bronze_documents.py          # Auto Loader로 PDF 바이너리 수집
    ├── silver/
    │   └── silver_documents.py          # ai_parse_document()로 텍스트 추출
    └── gold/
        ├── gold_document_summary.py     # 문서별 요약 테이블
        ├── gold_category_metrics.py     # 일자별 집계 지표
        └── gold_document_ai_summary.py  # AI 요약 (ai_query)
```

---

## 파이프라인 콘솔 설정 가이드

콘솔에서 수동으로 파이프라인을 생성·설정하는 방법입니다.

### 1. 파이프라인 생성

1. 왼쪽 메뉴에서 **Lakeflow** → **Pipelines** 클릭
2. 우상단 **Create pipeline** 버튼 클릭
3. Pipeline name 입력: `databricks-pipeline-sample`

### 2. 소스 파일 추가

1. **Source code** 섹션에서 **Add source code** 클릭
2. 파일 탐색기에서 아래 경로의 폴더 선택 (glob 패턴 자동 적용)
   ```
   /Users/{username}/databricks-pipeline-sample_4100f328/pipeline/
   ```
3. 저장 시 glob 패턴이 아래와 같이 설정됨:
   ```
   /Users/{username}/databricks-pipeline-sample_4100f328/pipeline/**
   ```

### 3. Catalog / Schema 설정

1. **Destination** 섹션에서 **Storage options** 클릭
2. **Catalog** 항목에 `developer팀` 입력
3. **Target schema** 항목에 `default` 입력

### 4. Compute 설정

| 항목 | 설정값 |
|---|---|
| Compute type | Serverless |
| Photon acceleration | 활성화 (체크) |
| Channel | Current |

1. **Compute** 섹션 → **Serverless** 선택
2. **Photon acceleration** 체크박스 활성화

### 5. Configuration 추가 (S3 경로)

1. **Advanced** 섹션 → **Configuration** 클릭
2. **Add configuration** 버튼 클릭
3. 아래 키-값 입력:

| Key | Value |
|---|---|
| `s3_landing_path` | `s3://databricks-storage-7474657118263619/unity-catalog/7474657118263619/landing/documents/` |

> **참고**: 이 값은 `bronze_documents.py`에서 `spark.conf.get("s3_landing_path", ...)` 으로 참조됩니다. S3 경로 변경 시 코드 수정 없이 이 값만 바꾸면 됩니다.

### 6. 파이프라인 저장 및 실행

1. 우상단 **Save** 클릭 후 **Start** 클릭 (일반 업데이트)
2. 스키마 변경 시: **Start** 옆 화살표(▼) → **Full refresh** 선택

---

## 대시보드 콘솔 설정 가이드

콘솔에서 수동으로 AI/BI 대시보드를 생성·설정하는 방법입니다.

### 1. 대시보드 생성

1. 왼쪽 메뉴에서 **SQL** → **Dashboards** 클릭
2. 우상단 **Create dashboard** 버튼 클릭
3. 대시보드 이름 입력: `보험 문서 파이프라인 결과 대시보드`

### 2. 데이터셋 추가

대시보드 편집 화면 하단의 **Data** 탭에서 각 데이터셋을 추가합니다.

**데이터셋 1 — Gold Document Summary**

1. **Create dataset** 클릭
2. Dataset name: `Gold Document Summary`
3. 아래 SQL 입력 후 **Save**:
   ```sql
   SELECT * FROM `developer팀`.`default`.`gold_document_summary`
   ```

**데이터셋 2 — Gold Category Metrics**

1. **Create dataset** 클릭
2. Dataset name: `Gold Category Metrics`
3. 아래 SQL 입력 후 **Save**:
   ```sql
   SELECT * FROM `developer팀`.`default`.`gold_category_metrics`
   ```

**데이터셋 3 — Gold Document AI Summary**

1. **Create dataset** 클릭
2. Dataset name: `Gold Document AI Summary`
3. 아래 SQL 입력 후 **Save**:
   ```sql
   SELECT * FROM `developer팀`.`default`.`gold_document_ai_summary`
   ```

### 3. 위젯 추가

대시보드 캔버스에서 **Add widget** 버튼으로 아래 위젯을 추가합니다.

#### 3-1. Counter 위젯 3개 (1행에 나란히 배치)

| 위젯명 | 데이터셋 | Measure |
|---|---|---|
| 총 문서 수 | Gold Document Summary | COUNT DISTINCT `document_id` |
| 총 페이지 수 | Gold Category Metrics | SUM `total_pages` |
| 총 글자 수 | Gold Category Metrics | SUM `total_chars` |

각 Counter 위젯 설정 방법:
1. 위젯 유형 **Counter** 선택
2. 해당 데이터셋 선택
3. **Value** 항목에서 컬럼과 집계 함수 선택

#### 3-2. Table 위젯 — 문서별 요약 정보

1. 위젯 유형 **Table** 선택
2. 데이터셋: `Gold Document Summary`
3. 표시할 컬럼 선택:
   - `document_id`
   - `source_file_name`
   - `page_count`
   - `total_chars`
   - `file_date`

#### 3-3. Bar Chart 위젯 — 문서별 페이지 수

1. 위젯 유형 **Bar** 선택
2. 데이터셋: `Gold Document Summary`
3. X축: `source_file_name`
4. Y축: `page_count` (SUM)

#### 3-4. Table 위젯 — AI 요약 결과

1. 위젯 유형 **Table** 선택
2. 데이터셋: `Gold Document AI Summary`
3. 표시할 컬럼 선택:
   - `document_id`
   - `page_count`
   - `ai_summary`

### 4. 대시보드 게시 (선택)

다른 사용자와 공유하려면:

1. 우상단 **Publish** 클릭
2. 권한 선택:
   - **Run as owner**: 대시보드 소유자 권한으로 실행 (공유 편리)
   - **Run as viewer**: 각 조회자 권한으로 실행 (보안 강화)
3. **Publish** 클릭 후 생성된 URL 공유

---

## 실행 방법

1. S3 Landing Zone에 PDF 파일 업로드
   ```
   s3://databricks-storage-7474657118263619/unity-catalog/7474657118263619/landing/documents/
   ```
2. 파이프라인 **Start** 클릭 (incremental update)
3. 스키마 변경 시 **Full Refresh** 선택 후 실행
4. 파이프라인 완료 후 대시보드에서 결과 확인

---

## 자동 트리거

이 파이프라인은 Lakeflow Job의 **File Arrival 트리거**로 S3에 파일이 도착하면 자동 실행됩니다.

| 항목 | 설정값 |
|---|---|
| Job 이름 | `databricks-pipeline-sample-batch` |
| Job ID | `504229728474154` |
| 트리거 방식 | File Arrival |
| 감시 경로 | `s3://databricks-storage-7474657118263619/unity-catalog/7474657118263619/landing/documents/` |
| 최소 실행 간격 | 600초 (10분) |
| 마지막 변경 후 대기 | 300초 (5분) |

S3 Landing Zone에 새 PDF가 적재되면, 마지막 파일 변경 후 5분 대기 → 파이프라인 자동 실행 → Bronze → Silver → Gold 전체 레이어를 처리합니다.

> **참고**: 트리거 설정을 변경하려면 Lakeflow Jobs 콘솔에서 해당 Job의 Trigger를 수정하세요.

---

## 사용 AI 함수

| 함수 | 위치 | 설명 |
|---|---|---|
| `ai_parse_document()` | silver_documents.py | PDF 바이너리에서 구조화된 텍스트·요소 추출  |
| `ai_query()` | gold_document_ai_summary.py | `databricks-meta-llama-3-3-70b-instruct` 모델로 한국어 요약 생성 |
