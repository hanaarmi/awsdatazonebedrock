# AWS DataZone 메타데이터 자동 생성기

AWS DataZone의 테이블 메타데이터를 Bedrock AI를 활용하여 자동으로 생성하는 프로그램입니다.

## 설치 방법

### 1. 프로젝트 클론
```bash
git clone [repository URL]
cd [repository name]
```

### 2. Python 가상환경 설정
Pipenv를 사용하여 프로젝트의 의존성을 관리합니다.

```bash
# Pipenv 설치 (미설치 시)
pip install pipenv

# 가상환경 생성 및 의존성 설치
pipenv install
```

### 3. 환경변수 설정
`.env.example` 파일을 `.env`로 복사하고 필요한 값들을 입력합니다.

```bash
cp .env.example .env
```

`.env` 파일을 열어 다음 값들을 설정합니다:
```plaintext
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DATAZONE_REGION=ap-northeast-2
AWS_BEDROCK_REGION=us-west-2
BEDROCK_MODEL_ID=anthropic.claude-3-sonnet-20240229-v1:0
```

### 4. 스키마 설명 파일 준비
`schemadesc.txt` 파일에 테이블 스키마에 대한 설명을 작성합니다. 이 설명은 AI가 컬럼 메타데이터를 생성할 때 컨텍스트로 사용됩니다.

## 실행 방법

가상환경을 활성화하고 프로그램을 실행합니다:
```bash
pipenv shell
python main.py
```

## 실행 결과

### 실행 전
![실행 전 DataZone 메타데이터](./bef.png)

### 실행 후
![실행 후 DataZone 메타데이터](./aft.png)

## 주요 기능

- AWS DataZone의 테이블 메타데이터 자동 생성
- Bedrock AI를 활용한 컬럼 설명 및 비즈니스 이름 생성
- 생성된 메타데이터의 자동 업데이트

## 환경 요구사항

- Python 3.12 이상
- AWS 계정 및 적절한 권한
- AWS DataZone 도메인 및 에셋 설정
- Bedrock AI 사용 권한
