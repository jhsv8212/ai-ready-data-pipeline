# File Arrival Trigger 트러블슈팅 가이드

## 개요

- **Job**: `databricks-pipeline-sample-batch` (ID: 504229728474154)
- **트리거 유형**: File Arrival
- **모니터링 경로**: `s3://databricks-storage-7474657118263619/unity-catalog/7474657118263619/landing/documents/`
- **External Location**: `developer팀`
- **Storage Credential**: `developer팀`
- **IAM Role**: `arn:aws:iam::144149479695:role/databricks-storage-role-7474657118263619`

---

## 발생한 문제

### 에러 메시지

```
Failed to provision file events resources during sns.createTopic operation.
User: arn:aws:sts::144149479695:assumed-role/databricks-storage-role-7474657118263619/...
is not authorized to perform: SNS:CreateTopic on resource: arn:aws:sns:us-east-2:144149479695:csms-topic-by-path-...
because no permissions boundary allows the SNS:CreateTopic action
```

### 근본 원인

Databricks IAM Role (`databricks-storage-role-7474657118263619`)의 **Permissions Boundary**가 `SNS:CreateTopic` 액션을 허용하지 않음. IAM Policy에는 권한이 있지만, Permissions Boundary가 더 제한적으로 설정되어 있어 Automatic 모드에서 파일 이벤트 리소스(SNS Topic, SQS Queue)를 자동 생성할 수 없음.

### 추가 제약

Permissions Boundary 정책이 **Databricks 관리 계정** 소유이므로 직접 수정 불가:
```
Cannot create versions for policies outside your own account.
```

---

## 해결 방법: Provided Queue 방식

Automatic 모드 대신 SQS 큐를 직접 생성하여 제공하는 **Provided** 방식으로 전환.

### Step 1: SQS 큐 생성

- **리전**: us-east-2
- **큐 이름**: `databricks-sqs`
- **큐 URL**: `https://sqs.us-east-2.amazonaws.com/144149479695/databricks-sqs`
- **큐 ARN**: `arn:aws:sqs:us-east-2:144149479695:databricks-sqs`

### Step 2: SQS Access Policy 설정

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowS3EventNotification",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:us-east-2:144149479695:databricks-sqs",
      "Condition": {
        "ArnLike": {
          "aws:SourceArn": "arn:aws:s3:::databricks-storage-7474657118263619"
        }
      }
    },
    {
      "Sid": "AllowDatabricksReadMessages",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::144149479695:role/databricks-storage-role-7474657118263619"
      },
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:SendMessage",
        "sqs:PurgeQueue",
        "sqs:GetQueueAttributes",
        "sqs:GetQueueUrl"
      ],
      "Resource": "arn:aws:sqs:us-east-2:144149479695:databricks-sqs"
    }
  ]
}
```

> **참고**: 동일 계정 내 Resource-based policy(SQS Access Policy)는 Permissions Boundary와 무관하게 접근을 허용함.

### Step 3: S3 Event Notification 설정

S3 버킷 `databricks-storage-7474657118263619` → Properties → Event notifications:

| 항목 | 값 |
|------|----|
| Event name | `databricks-file-events` |
| Prefix | `unity-catalog/7474657118263619/` |
| Event types | `All object create events`, `All object removal events`, `Object Lifecycle expiration` |
| Destination | SQS queue → `arn:aws:sqs:us-east-2:144149479695:databricks-sqs` |

> **주의**: SQS Access Policy의 `AllowS3EventNotification` 구문을 먼저 추가해야 S3 이벤트 알림 생성이 성공함. 그렇지 않으면 "Unable to validate the following destination configurations" 에러 발생.

### Step 4: External Location 업데이트 (API)

UI에서 "Nothing to update" 에러가 발생할 수 있으므로 API로 직접 업데이트:

```python
import requests

url = f"{host}/api/2.1/unity-catalog/external-locations/developer%ED%8C%80"

payload = {
    "enable_file_events": True,
    "skip_validation": True,  # 초기 설정 시 validation skip 가능
    "file_event_queue": {
        "provided_sqs": {
            "queue_url": "https://sqs.us-east-2.amazonaws.com/144149479695/databricks-sqs"
        }
    }
}

response = requests.patch(url, json=payload, headers=headers)
```

### Step 5: 트리거 새로고침

External Location 변경 후 트리거 서비스가 캐시된 설정을 사용할 수 있으므로, Job 트리거를 일시 중지(PAUSED) → 재활성화(UNPAUSED)하여 새 설정을 반영:

```json
// editAsset 또는 API 사용
{"trigger": {"pause_status": "PAUSED", "file_arrival": {...}}}
// 잠시 후
{"trigger": {"pause_status": "UNPAUSED", "file_arrival": {...}}}
```

---

## 검증 방법

1. **Test Connection**: Catalog Explorer → External Locations → `developer팀` → Test connection
   - "파일 이벤트 읽기" 성공 확인
   - "건너뜀"은 큐에 메시지가 없다는 의미 (실패 아님)

2. **트리거 동작 확인**: 모니터링 경로에 새 파일 업로드 후 60~120초 내 Job 자동 실행 확인

3. **Validation 재실행** (skip_validation 없이):
   ```python
   payload = {
       "enable_file_events": True,
       "skip_validation": False,
       "file_event_queue": {
           "provided_sqs": {
               "queue_url": "https://sqs.us-east-2.amazonaws.com/144149479695/databricks-sqs"
           }
       }
   }
   # HTTP 200 반환 시 정상
   ```

---

## 트러블슈팅 체크리스트

| 증상 | 원인 | 해결 |
|------|------|------|
| `SNS:CreateTopic` 권한 에러 | Permissions Boundary 제한 | Provided Queue 방식으로 전환 |
| `Unable to validate destination` | SQS Access Policy 미설정 | S3 → SQS SendMessage 허용 추가 |
| `READ_MESSAGE permissions` 에러 | IAM Role이 SQS 읽기 불가 | SQS Access Policy에 Databricks Role 허용 추가 |
| `Failed to parse event message` | 큐에 비정상 메시지 존재 | SQS Purge → 새 파일 업로드 |
| `Nothing to update` (UI/API) | 파일 이벤트 미활성 상태에서 큐만 변경 시도 | `enable_file_events: true` 포함하여 업데이트 |
| 트리거 미동작 (설정 완료 후) | 트리거 서비스 캐시 | Job 트리거 PAUSE → UNPAUSE |

---

## 최종 결과

**해결 완료** ✅ (2026-07-30)

파일 도착 트리거가 정상 동작 확인됨:
- S3 파일 업로드 → S3 Event Notification → SQS 큐 → Databricks 트리거 → Job 자동 실행
- 파일 업로드 후 약 **2~3분** 내 트리거 발동 (설정값 기반 정상 지연)

### 트리거 동작 타이밍

| 구간 | 소요 시간 | 설명 |
|------|-----------|------|
| S3 → SQS | 수 초 | S3 Event Notification이 SQS로 이벤트 전송 |
| wait_after_last_change_seconds | 60초 | 추가 파일 업로드 대기 (배치 처리 목적) |
| min_time_between_triggers_seconds | 60초 | 이전 트리거 이후 최소 대기 |
| **총 예상 소요** | **~2~3분** | 파일 업로드 후 Job 실행까지 |

### 작업 순서 요약

1. SQS 큐 생성 (`databricks-sqs`)
2. SQS Access Policy 설정 (S3 SendMessage + Databricks ReadMessage)
3. S3 Event Notification 생성 (prefix: `unity-catalog/7474657118263619/`)
4. External Location API 업데이트 (`enable_file_events: true` + `provided_sqs`)
5. Job 트리거 PAUSE → UNPAUSE (캐시 새로고침)
6. 새 파일 업로드 → 트리거 정상 동작 확인

---

## 참고 자료

- [Databricks 외부 위치 파일 이벤트 설정](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/manage-external-locations/)
- [파일 이벤트 FAQ](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/file-events-faq/)
- [파일 도착 트리거 설정](https://docs.databricks.com/aws/en/jobs/trigger.html)
