import boto3
import json
from typing import Optional, Dict, Any, List
from datetime import datetime


class DataZoneManager:
    def __init__(self, domain_id: str, region_name: str = 'ap-northeast-2'):
        self.client = boto3.client('datazone', region_name=region_name)
        self.domain_id = domain_id
        # Initialize form type revisions
        self.form_type_revisions = self._get_form_type_revisions()

    def _get_form_type_revisions(self) -> Dict[str, str]:
        """Get latest revision numbers for form types"""
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
        Get the latest content of an asset from DataZone including both table structure and metadata
        """
        try:
            response = self.client.get_asset(
                domainIdentifier=domain_id,
                identifier=asset_id
            )

            glue_table_content = None
            column_metadata_content = None

            # 각 form의 content 가져오기
            for form in response['formsOutput']:
                if form['formName'] == 'GlueTableForm':
                    glue_table_content = json.loads(form['content'])
                elif form['formName'] == 'ColumnBusinessMetadataForm':
                    column_metadata_content = json.loads(form['content'])

            if not glue_table_content or not column_metadata_content:
                return None

            # 메타데이터 정보를 컬럼 정보와 합치기
            metadata_by_column = {
                meta['columnIdentifier']: meta
                for meta in column_metadata_content['columnsBusinessMetadata']
            }

            # 컬럼 정보 업데이트
            for column in glue_table_content['columns']:
                column_name = column['columnName']
                if column_name in metadata_by_column:
                    metadata = metadata_by_column[column_name]
                    column['description'] = metadata.get('description')
                    column['businessName'] = metadata.get('name')

            return {
                'glueTableContent': glue_table_content,
                'columnMetadataContent': column_metadata_content
            }

        except Exception as e:
            print(f"Error getting asset: {str(e)}")
            return None

    def create_asset_revision(
        self,
        domain_id: str,
        asset_id: str,
        modified_content: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Create a new revision of an asset with modified content
        """
        try:
            glue_table_content = modified_content['glueTableContent']
            column_metadata_content = modified_content['columnMetadataContent']

            # GlueTableForm에서는 dataType과 columnName만 유지
            columns_metadata = {}  # Store original metadata
            for column in glue_table_content['columns']:
                # Store metadata before removing
                columns_metadata[column['columnName']] = {
                    # Use columnName as default
                    'businessName': column.get('businessName', column['columnName']),
                    'description': column.get('description', '')
                }
                # Remove from GlueTableForm
                if 'businessName' in column:
                    del column['businessName']
                if 'description' in column:
                    del column['description']

            # 메타데이터 업데이트 (ColumnBusinessMetadataForm)
            updated_metadata = []
            for column in glue_table_content['columns']:
                column_name = column['columnName']
                metadata = columns_metadata[column_name]
                metadata_entry = {
                    'columnIdentifier': column_name,
                    # Use stored businessName
                    'name': metadata['businessName'],
                    'description': metadata['description'] if metadata['description'] is not None else ''
                }
                updated_metadata.append(metadata_entry)

            print("\nVerifying metadata before API call:")
            print(json.dumps(updated_metadata, indent=2))

            column_metadata_content['columnsBusinessMetadata'] = updated_metadata

            # Generate revision name with timestamp
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            revision_name = f"Made by script - {current_time}"

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

            response = self.client.create_asset_revision(
                domainIdentifier=domain_id,
                identifier=asset_id,
                formsInput=forms_input,
                name=revision_name
            )

            print(f"Successfully created new revision: {
                  response['revision']} with name: {revision_name}")
            return response

        except Exception as e:
            print(f"Error creating asset revision: {str(e)}")
            return None


def main():
    # Configuration
    DOMAIN_ID = 'dzd_brmpgriuef9es8'
    ASSET_ID = 'cclm9ky03uwl5k'

    # Initialize DataZone manager with domain_id
    dzm = DataZoneManager(domain_id=DOMAIN_ID)

    # Get latest content
    content = dzm.get_latest_asset_content(DOMAIN_ID, ASSET_ID)

    if content:
        glue_content = content['glueTableContent']

        # 수정 전 모든 컬럼 정보 출력
        print("Current columns:")
        print(json.dumps(glue_content['columns'],
              indent=2, ensure_ascii=False))

        # 첫 번째 컬럼의 description 수정
        if glue_content['columns']:
            first_column = glue_content['columns'][0]
            first_column['description'] = "Updated description"

        # 수정 후 모든 컬럼 정보 출력
        print("\nModified columns:")
        print(json.dumps(glue_content['columns'],
              indent=2, ensure_ascii=False))

        # 새로운 리비전 생성
        dzm.create_asset_revision(DOMAIN_ID, ASSET_ID, content)
    else:
        print("Failed to get asset content")


if __name__ == "__main__":
    main()
