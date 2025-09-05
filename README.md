# Maritime Cargo Risk Analysis - Auto Restart System

## 개요
해양 화물 위험 분석 및 응급 의학 가이드라인 자동 생성 시스템

## 주요 기능
- **자동 재시작**: 5분 이상 비활성 시 자동 중단 후 재시작
- **진행 상황 보존**: 중단되어도 이전 작업 손실 없음
- **선박 의약품 기반**: 실제 해상법 규정 의약품으로 응급처치 가이드라인 생성
- **배치 처리**: 안정적인 대용량 데이터 처리

## 필수 파일
- `auto_restart_analysis.py` - 메인 실행 파일
- `cargolist.csv` - 화물 리스트 (ID_No, Guide_No, Name_of_Material 컬럼 필요)
- `medi.md` - 선박 의약품 정보
- `requirements.txt` - Python 종속성

## 설치 및 실행

### 1. 환경 설정
```bash
# Python 패키지 설치
pip install -r requirements.txt

# Gemini API 키 환경변수 설정
export GEMINI_API_KEY="your_api_key_here"
```

### 2. 실행
```bash
python auto_restart_analysis.py
```

## 출력 파일
- `maximum_data_batch_N_YYYYMMDD_HHMM.csv` - 배치별 분석 결과

## 컬럼 구조
- Cargo: 화물명
- Stage: 분석 단계 (Risk Analysis, Emergency Procedures, etc.)
- Category: 위험/처치 유형
- Description: 상세 설명
- Detail1-3: 추가 상세 정보

## 자동 재시작 기능
- 5분 이상 비활성 감지 시 자동 중단
- 재실행하면 중단된 지점부터 자동 계속
- 처리된 화물은 자동으로 건너뜀

## 주의사항
- API 키는 환경변수로 설정 필수
- 인터넷 연결 필요 (Gemini API 호출)
- 대용량 처리 시 충분한 디스크 공간 확보