import boto3
import json
from typing import Optional, Dict, Any, List
from datetime import datetime
import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()


class DataZoneManager:
    def __init__(self, domain_id: str):
        # 환경 변수에서 리전 정보 가져오기
        region_datazone = os.getenv('AWS_DATAZONE_REGION', 'ap-northeast-2')
        region_bedrock = os.getenv('AWS_BEDROCK_REGION', 'us-west-2')

        # AWS 클라이언트 초기화
        # DataZone 클라이언트 설정
        self.client = boto3.client('datazone',
                                   region_name=region_datazone,
                                   aws_access_key_id=os.getenv(
                                       'AWS_ACCESS_KEY_ID'),
                                   aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

        # Bedrock 클라이언트 설정 (AI 모델 사용을 위함)
        self.bedrock = boto3.client('bedrock-runtime',
                                    region_name=region_bedrock,
                                    aws_access_key_id=os.getenv(
                                        'AWS_ACCESS_KEY_ID'),
                                    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

        self.domain_id = domain_id
        # 폼 타입 리비전 초기화
        self.form_type_revisions = self._get_form_type_revisions()

    def _get_form_type_revisions(self) -> Dict[str, str]:
        """폼 타입의 최신 리비전 번호를 가져오는 메서드"""
        try:
            revisions = {}
            # Get GlueTableFormType revision
            print("Fetching GlueTableFormType revision...")
            glue_response = self.client.get_form_type(
                domainIdentifier=self.domain_id,
                formTypeIdentifier='amazon.datazone.GlueTableFormType'
            )
            print(f"GlueTableFormType response: {glue_response}")
            revisions['GlueTableForm'] = str(glue_response['revision'])

            # Get ColumnBusinessMetadataFormType revision
            print("Fetching ColumnBusinessMetadataFormType revision...")
            metadata_response = self.client.get_form_type(
                domainIdentifier=self.domain_id,
                formTypeIdentifier='amazon.datazone.ColumnBusinessMetadataFormType'
            )
            print(f"ColumnBusinessMetadataFormType response: {
                  metadata_response}")
            revisions['ColumnBusinessMetadataForm'] = str(
                metadata_response['revision'])

            print(f"Final revisions: {revisions}")
            return revisions
        except Exception as e:
            print(f"Error getting form type revisions: {str(e)}")
            return {}

    def get_latest_asset_content(self, domain_id: str, asset_id: str) -> Optional[Dict[str, Any]]:
        """
        DataZone에서 테이블 구조와 메타데이터를 포함한 최신 에셋 내용을 가져오는 메서드
        """
        try:
            response = self.client.get_asset(
                domainIdentifier=domain_id,
                identifier=asset_id
            )

            glue_table_content = None
            column_metadata_content = None

            # 각 폼의 내용 가져오기
            # GlueTableForm: 테이블의 기본 구조 정보
            # ColumnBusinessMetadataForm: 컬럼의 비즈니스 메타데이터 정보
            for form in response['formsOutput']:
                if form['formName'] == 'GlueTableForm':
                    glue_table_content = json.loads(form['content'])
                elif form['formName'] == 'ColumnBusinessMetadataForm':
                    column_metadata_content = json.loads(form['content'])

            if not glue_table_content or not column_metadata_content:
                return None

            # 메타데이터 정보를 컬럼별로 정리하여 딕셔너리 생성
            metadata_by_column = {
                meta['columnIdentifier']: meta
                for meta in column_metadata_content['columnsBusinessMetadata']
            }

            # 각 컬럼에 대한 메타데이터 정보 업데이트
            for column in glue_table_content['columns']:
                column_name = column['columnName']
                if column_name in metadata_by_column:
                    metadata = metadata_by_column[column_name]
                    # 설명과 비즈니스 이름 추가
                    column['description'] = metadata.get('description')
                    column['businessName'] = metadata.get('name')

            return {
                'glueTableContent': glue_table_content,
                'columnMetadataContent': column_metadata_content
            }

        except Exception as e:
            print(f"에셋 정보 가져오기 실패: {str(e)}")
            return None

    def create_asset_revision(
        self,
        domain_id: str,
        asset_id: str,
        modified_content: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        수정된 내용으로 에셋의 새로운 리비전을 생성하는 메서드
        """
        try:
            glue_table_content = modified_content['glueTableContent']
            column_metadata_content = modified_content['columnMetadataContent']

            # 컬럼 메타데이터 임시 저장
            # GlueTableForm에는 기본 정보만 남기고 메타데이터는 별도로 관리
            columns_metadata = {}
            for column in glue_table_content['columns']:
                # 메타데이터 정보 임시 저장
                columns_metadata[column['columnName']] = {
                    'businessName': column.get('businessName', column['columnName']),
                    'description': column.get('description', '')
                }
                # GlueTableForm에서 메타데이터 필드 제거
                if 'businessName' in column:
                    del column['businessName']
                if 'description' in column:
                    del column['description']

            # ColumnBusinessMetadataForm 업데이트
            updated_metadata = []
            for column in glue_table_content['columns']:
                column_name = column['columnName']
                metadata = columns_metadata[column_name]
                metadata_entry = {
                    'columnIdentifier': column_name,
                    'name': metadata['businessName'],
                    'description': metadata['description'] if metadata['description'] is not None else ''
                }
                updated_metadata.append(metadata_entry)

            print("\n메타데이터 검증:")
            print(json.dumps(updated_metadata, indent=2))

            # 메타데이터 컨텐츠 업데이트
            column_metadata_content['columnsBusinessMetadata'] = updated_metadata

            # 타임스탬프를 포함한 리비전 이름 생성
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            revision_name = f"스크립트 생성 - {current_time}"

            # API 호출을 위한 폼 입력 데이터 준비
            forms_input = [
                {
                    'content': json.dumps(glue_table_content),
                    'formName': 'GlueTableForm',
                    'typeIdentifier': 'amazon.datazone.GlueTableFormType',
                    'typeRevision': self.form_type_revisions.get('GlueTableForm', '1')
                },
                {
                    'content': json.dumps(column_metadata_content),
                    'formName': 'ColumnBusinessMetadataForm',
                    'typeIdentifier': 'amazon.datazone.ColumnBusinessMetadataFormType',
                    'typeRevision': self.form_type_revisions.get('ColumnBusinessMetadataForm', '1')
                }
            ]

            # 새로운 리비전 생성
            response = self.client.create_asset_revision(
                domainIdentifier=domain_id,
                identifier=asset_id,
                formsInput=forms_input,
                name=revision_name
            )

            print(f"새로운 리비전 생성 완료: {
                  response['revision']}, 이름: {revision_name}")
            return response

        except Exception as e:
            print(f"에셋 리비전 생성 실패: {str(e)}")
            return None

    def generate_column_metadata(self, column_name: str, context: str) -> Dict[str, str]:
        """
        Bedrock AI 모델을 사용하여 컬럼의 비즈니스 이름과 설명을 생성하는 메서드
        """
        try:
            # 프롬프트 메시지 생성
            prompt = f"""Given the following column name and context, generate a business-friendly name and description:
            Column Name: {column_name}
            Context: {context}

            Please provide the output in the following JSON format:
            {{
                "businessName": "user-friendly name",
                "description": "detailed description"
            }}"""

            # Bedrock 요청 본문 준비
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 100,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }

            # AI 모델 호출
            response = self.bedrock.invoke_model(
                modelId=os.getenv('BEDROCK_MODEL_ID'),
                body=json.dumps(body),
                accept="application/json",
                contentType="application/json"
            )

            # 응답 처리
            response_body = json.loads(response['body'].read())

            # AI 모델 응답에서 JSON 형식의 결과 추출
            if ('content' in response_body and
                isinstance(response_body['content'], list) and
                len(response_body['content']) > 0 and
                    'text' in response_body['content'][0]):
                response_text = response_body['content'][0]['text']
                # 응답에서 JSON 부분 찾기
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    json_str = response_text[json_start:json_end]
                    generated_content = json.loads(json_str)
                    return generated_content

            # AI 모델 응답 실패시 기본값 반환
            return {
                "businessName": column_name,
                "description": ""
            }

        except Exception as e:
            print(f"메타데이터 생성 중 오류 발생: {str(e)}")
            return {
                "businessName": column_name,
                "description": ""
            }


def main():
    # 설정값
    DOMAIN_ID = '[Input your domain id]'
    ASSET_ID = '[Input your asset id]'

    # 스키마 설명 파일 읽기
    try:
        with open('schemadesc.txt', 'r', encoding='utf-8') as f:
            schema_context = f.read()
    except Exception as e:
        print(f"스키마 설명 파일 읽기 실패: {str(e)}")
        return

    # DataZone 매니저 초기화
    dzm = DataZoneManager(domain_id=DOMAIN_ID)

    # 최신 에셋 내용 가져오기
    content = dzm.get_latest_asset_content(DOMAIN_ID, ASSET_ID)

    if content:
        glue_content = content['glueTableContent']

        # 각 컬럼에 대한 메타데이터 업데이트
        for column in glue_content['columns']:
            print(f"\n컬럼 처리 중: {column['columnName']}")

            # Bedrock을 사용하여 메타데이터 생성
            metadata = dzm.generate_column_metadata(
                column['columnName'],
                schema_context
            )

            # 컬럼 메타데이터 업데이트
            column['businessName'] = metadata['businessName']
            column['description'] = metadata['description']

            print(f"생성된 메타데이터: {json.dumps(
                metadata, indent=2, ensure_ascii=False)}")

        # 새로운 리비전 생성
        result = dzm.create_asset_revision(DOMAIN_ID, ASSET_ID, content)

        if result:
            print("모든 컬럼 메타데이터 업데이트 완료")
        else:
            print("새로운 리비전 생성 실패")
    else:
        print("에셋 내용 가져오기 실패")


if __name__ == "__main__":
    main()
