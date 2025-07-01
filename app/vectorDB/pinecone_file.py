import pandas as pd
from openai import OpenAI
import numpy as np
import json
from typing import List, Dict, Any
import time
import datetime
from tqdm import tqdm
import os

# Pinecone 버전 호환성 처리
try:
    from pinecone import Pinecone
    PINECONE_NEW_VERSION = True
    print("📦 최신 Pinecone 라이브러리 감지")
except ImportError:
    import pinecone
    PINECONE_NEW_VERSION = False
    print("📦 구버전 Pinecone 라이브러리 감지")

class CSVToPineconeBuilder:
    def __init__(self, pinecone_api_key: str, openai_api_key: str):
        """
        CSV 파일을 Pinecone 벡터 DB로 구축하는 클래스
        """
        self.pinecone_api_key = pinecone_api_key
        self.openai_api_key = openai_api_key

        # OpenAI 클라이언트 초기화
        self.openai_client = OpenAI(api_key=self.openai_api_key)

        # Pinecone 초기화 (버전별 처리)
        self.pinecone_initialized = False
        
        if PINECONE_NEW_VERSION:
            # 최신 버전 방식
            try:
                self.pc = Pinecone(api_key=self.pinecone_api_key)
                indexes = self.pc.list_indexes()
                print(f"✅ Pinecone 최신 버전으로 초기화 성공")
                print(f"📋 발견된 인덱스: {[idx.name for idx in indexes.indexes] if hasattr(indexes, 'indexes') else []}")
                self.pinecone_initialized = True
            except Exception as e:
                print(f"❌ 최신 버전 초기화 실패: {e}")
        else:
            # 구버전 방식
            init_methods = [
                lambda: pinecone.init(api_key=self.pinecone_api_key),
                lambda: pinecone.init(
                    api_key=self.pinecone_api_key,
                    environment="us-east1-gcp"
                ),
                lambda: pinecone.init(
                    api_key=self.pinecone_api_key,
                    environment="us-east-1"
                )
            ]
            
            for i, init_method in enumerate(init_methods):
                try:
                    init_method()
                    indexes = pinecone.list_indexes()
                    print(f"✅ Pinecone 구버전으로 초기화 성공 (방법 {i+1})")
                    print(f"📋 발견된 인덱스: {indexes}")
                    self.pinecone_initialized = True
                    break
                except Exception as e:
                    print(f"⚠️ 초기화 방법 {i+1} 실패: {e}")
                    continue
        
        if not self.pinecone_initialized:
            raise Exception("Pinecone 초기화 실패")

        # 설정값
        self.embedding_model = "text-embedding-3-small"
        self.dimension = 1024
        self.metric = "cosine"

    def load_csv_files(self, file_paths: List[str]) -> pd.DataFrame:
        """CSV 파일들을 로드하고 합치기"""
        dataframes = []
        
        for file_path in file_paths:
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                    print(f"✅ {file_path} 로드 성공: {len(df)}행")
                    
                    # 파일명을 source 컬럼으로 추가
                    df['source_file'] = os.path.basename(file_path)
                    dataframes.append(df)
                    
                    # 컬럼 미리보기
                    print(f"📋 컬럼: {list(df.columns)}")
                    
                except Exception as e:
                    print(f"❌ {file_path} 로드 실패: {e}")
                    # UTF-8이 안되면 다른 인코딩 시도
                    try:
                        df = pd.read_csv(file_path, encoding='cp949')
                        print(f"✅ {file_path} 로드 성공 (cp949): {len(df)}행")
                        df['source_file'] = os.path.basename(file_path)
                        dataframes.append(df)
                    except Exception as e2:
                        print(f"❌ {file_path} 완전 실패: {e2}")
            else:
                print(f"❌ 파일을 찾을 수 없음: {file_path}")
        
        if dataframes:
            # 모든 DataFrame 합치기
            combined_df = pd.concat(dataframes, ignore_index=True)
            print(f"🔗 총 {len(combined_df)}개 레코드 결합 완료")
            
            # 데이터 미리보기
            print("\n📄 데이터 미리보기:")
            print(combined_df.head())
            print(f"\n📊 컬럼 정보:")
            print(combined_df.info())
            
            return combined_df
        else:
            print("❌ 로드된 CSV 파일이 없습니다.")
            return pd.DataFrame()

    def create_embedding_text(self, row: pd.Series) -> str:
        """DataFrame 행을 임베딩할 텍스트로 변환"""
        text_parts = []
        
        # 주요 텍스트 컬럼들을 찾아서 결합
        # 일반적인 컬럼명들 확인
        text_columns = []
        
        for col in row.index:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in [
                'task', 'work', 'content', 'description', 'summary', 
                '업무', '내용', '설명', '요약', 'done', 'activity',
                'project', 'report', 'result'
            ]):
                if pd.notna(row[col]) and str(row[col]).strip():
                    text_columns.append(col)
        
        # 텍스트 컬럼이 있으면 사용
        for col in text_columns:
            if pd.notna(row[col]):
                text_parts.append(f"{col}: {row[col]}")
        
        # 다른 중요한 정보들도 추가
        for col in row.index:
            if col not in text_columns and pd.notna(row[col]):
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in [
                    'user', 'id', 'date', 'year', 'quarter', 'organization',
                    '사용자', '날짜', '연도', '분기', '조직'
                ]):
                    text_parts.append(f"{col}: {row[col]}")
        
        # 소스 파일 정보 추가
        if 'source_file' in row.index:
            text_parts.append(f"출처: {row['source_file']}")
        
        return " | ".join(text_parts)

    def get_embedding(self, text: str) -> List[float]:
        """OpenAI API를 사용해 텍스트 임베딩 생성"""
        try:
            response = self.openai_client.embeddings.create(
                model=self.embedding_model,
                input=text,
                dimensions=self.dimension
            )
            return response.data[0].embedding
        except Exception as e:
            print(f"❌ 임베딩 생성 실패: {e}")
            return None

    def create_new_index(self) -> bool:
        """새로운 Pinecone 인덱스 생성 (버전별 처리)"""
        try:
            # 기존 인덱스 목록 확인
            if PINECONE_NEW_VERSION:
                indexes_response = self.pc.list_indexes()
                existing_indexes = [idx.name for idx in indexes_response.indexes] if hasattr(indexes_response, 'indexes') else []
            else:
                existing_indexes = pinecone.list_indexes()
            
            print(f"📋 기존 인덱스: {existing_indexes}")
            
            # 새 인덱스명 생성
            now_str = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            self.index_name = f"skore-{now_str}"
            
            print(f"🔨 새 인덱스 '{self.index_name}' 생성 중...")
            
            try:
                if PINECONE_NEW_VERSION:
                    # 최신 버전 방식
                    self.pc.create_index(
                        name=self.index_name,
                        dimension=self.dimension,
                        metric=self.metric,
                        spec={
                            'serverless': {
                                'cloud': 'aws',
                                'region': 'us-east-1'
                            }
                        }
                    )
                else:
                    # 구버전 방식
                    pinecone.create_index(
                        name=self.index_name,
                        dimension=self.dimension,
                        metric=self.metric
                    )
                
                print(f"✅ 인덱스 생성 성공!")
                
                # 초기화 대기
                print("⏳ 인덱스 초기화 대기 중... (60초)")
                time.sleep(60)
                
                # 인덱스 연결
                if PINECONE_NEW_VERSION:
                    self.index = self.pc.Index(self.index_name)
                else:
                    self.index = pinecone.Index(self.index_name)
                
                # 네임스페이스 설정
                self.namespace = f"weekly_cloud/esg-{now_str}"
                print(f"📛 네임스페이스: {self.namespace}")
                
                return True
                
            except Exception as create_error:
                print(f"❌ 인덱스 생성 실패: {create_error}")
                
                # pod 제한 에러인 경우
                if "max pods" in str(create_error).lower():
                    print("\n💡 해결 방법:")
                    print("1. 기존 인덱스 삭제")
                    print("2. 유료 플랜 업그레이드")
                    print("3. Chroma 사용")
                    
                    if existing_indexes:
                        print(f"\n🗑️ 삭제 가능한 인덱스:")
                        for i, idx in enumerate(existing_indexes):
                            print(f"  {i+1}. {idx}")
                        
                        choice = input("삭제할 인덱스 번호 (Enter: 건너뛰기): ").strip()
                        
                        if choice.isdigit() and 1 <= int(choice) <= len(existing_indexes):
                            idx_to_delete = existing_indexes[int(choice)-1]
                            try:
                                if PINECONE_NEW_VERSION:
                                    self.pc.delete_index(idx_to_delete)
                                else:
                                    pinecone.delete_index(idx_to_delete)
                                
                                print(f"✅ '{idx_to_delete}' 삭제 완료")
                                
                                # 재시도
                                time.sleep(10)
                                
                                if PINECONE_NEW_VERSION:
                                    self.pc.create_index(
                                        name=self.index_name,
                                        dimension=self.dimension,
                                        metric=self.metric,
                                        spec={
                                            'serverless': {
                                                'cloud': 'aws',
                                                'region': 'us-east-1'
                                            }
                                        }
                                    )
                                    self.index = self.pc.Index(self.index_name)
                                else:
                                    pinecone.create_index(
                                        name=self.index_name,
                                        dimension=self.dimension,
                                        metric=self.metric
                                    )
                                    self.index = pinecone.Index(self.index_name)
                                
                                print(f"✅ 재생성 성공!")
                                time.sleep(60)
                                self.namespace = f"weekly_cloud/esg-{now_str}"
                                return True
                                
                            except Exception as delete_error:
                                print(f"❌ 삭제 실패: {delete_error}")
                
                return False
                
        except Exception as e:
            print(f"❌ 전체 과정 실패: {e}")
            return False

    def process_and_upload(self, df: pd.DataFrame, batch_size: int = 10):
        """DataFrame 처리 및 Pinecone 업로드"""
        print(f"🔄 {len(df)}개 레코드 처리 시작...")
        
        # 처리할 데이터 범위 선택
        print("\n처리 옵션:")
        print("1. 테스트 (50개)")
        print("2. 중간 (200개)")
        print("3. 전체 데이터")
        print("4. 사용자 정의")
        
        choice = input("선택 (1-4, 기본값: 1): ").strip()
        
        if choice == "2":
            df_subset = df.head(200)
        elif choice == "3":
            df_subset = df
        elif choice == "4":
            limit = input("처리할 개수 입력: ").strip()
            limit = int(limit) if limit.isdigit() else 50
            df_subset = df.head(limit)
        else:
            df_subset = df.head(50)
        
        print(f"📊 {len(df_subset)}개 레코드 처리 예정")
        
        vectors = []
        successful_uploads = 0
        failed_count = 0
        
        for idx, row in tqdm(df_subset.iterrows(), total=len(df_subset), desc="처리 중"):
            try:
                # 텍스트 생성
                text = self.create_embedding_text(row)
                if not text.strip():
                    failed_count += 1
                    continue
                
                # 임베딩 생성
                embedding = self.get_embedding(text)
                if embedding is None:
                    failed_count += 1
                    continue
                
                # 메타데이터 준비
                metadata = {
                    'text': text[:800],  # 크기 제한
                    'row_index': str(idx),
                    'source_file': str(row.get('source_file', 'unknown'))
                }
                
                # DataFrame의 주요 컬럼들을 메타데이터에 추가
                for col in row.index:
                    if pd.notna(row[col]) and col != 'source_file':
                        # 메타데이터 키 정리
                        clean_key = str(col).replace(' ', '_').lower()
                        metadata[clean_key] = str(row[col])[:100]  # 길이 제한
                
                # 벡터 데이터 준비
                vector_data = {
                    'id': f"skore-{idx}",
                    'values': embedding,
                    'metadata': metadata
                }
                
                vectors.append(vector_data)
                
                # 배치 업로드
                if len(vectors) >= batch_size:
                    try:
                        self.index.upsert(vectors=vectors, namespace=self.namespace)
                        successful_uploads += len(vectors)
                        print(f"✅ {successful_uploads}개 업로드 완료")
                        vectors = []
                        time.sleep(1)
                    except Exception as e:
                        print(f"❌ 배치 업로드 실패: {e}")
                        failed_count += len(vectors)
                        vectors = []
                
            except Exception as e:
                print(f"❌ 레코드 {idx} 처리 실패: {e}")
                failed_count += 1
                continue
        
        # 남은 벡터 업로드
        if vectors:
            try:
                self.index.upsert(vectors=vectors, namespace=self.namespace)
                successful_uploads += len(vectors)
                print(f"✅ 최종 {len(vectors)}개 업로드 완료")
            except Exception as e:
                print(f"❌ 최종 업로드 실패: {e}")
                failed_count += len(vectors)
        
        print(f"🎉 처리 완료!")
        print(f"  ✅ 성공: {successful_uploads}개")
        print(f"  ❌ 실패: {failed_count}개")
        
        return successful_uploads

    def get_stats(self):
        """인덱스 통계 확인"""
        try:
            stats = self.index.describe_index_stats()
            print("📊 최종 통계:")
            print(f"  - 인덱스명: {self.index_name}")
            print(f"  - 네임스페이스: {self.namespace}")
            print(f"  - 총 벡터 수: {stats.total_vector_count}")
            print(f"  - 네임스페이스별 통계:")
            for ns_name, ns_stats in stats.namespaces.items():
                print(f"    • {ns_name}: {ns_stats.vector_count}개")
            return stats
        except Exception as e:
            print(f"❌ 통계 조회 실패: {e}")
            return None

    def run_pipeline(self, csv_files: List[str]):
        """전체 파이프라인 실행"""
        print("🚀 CSV to Pinecone 벡터 DB 구축 시작")
        print("="*60)
        
        # 1. CSV 파일 로드
        df = self.load_csv_files(csv_files)
        if df.empty:
            return False, None, None
        
        # 2. 인덱스 생성
        if not self.create_new_index():
            return False, None, None
        
        # 3. 데이터 처리 및 업로드
        uploaded_count = self.process_and_upload(df)
        
        # 4. 통계 확인
        self.get_stats()
        
        if uploaded_count > 0:
            print("✅ 벡터 DB 구축 완료!")
            return True, self.index_name, self.namespace
        else:
            return False, None, None

# 검색 함수
def search_csv_data(query: str, pinecone_api_key: str, openai_api_key: str, 
                   index_name: str, namespace: str, top_k: int = 5):
    """CSV 데이터에서 검색"""
    try:
        print(f"🔍 검색: '{query}'")
        
        # 쿼리 임베딩
        client = OpenAI(api_key=openai_api_key)
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=query,
            dimensions=1024
        )
        query_embedding = response.data[0].embedding
        
        # Pinecone 검색 (버전별 처리)
        if PINECONE_NEW_VERSION:
            pc = Pinecone(api_key=pinecone_api_key)
            index = pc.Index(index_name)
        else:
            pinecone.init(api_key=pinecone_api_key)
            index = pinecone.Index(index_name)
        
        results = index.query(
            vector=query_embedding,
            top_k=top_k,
            namespace=namespace,
            include_metadata=True
        )
        
        print(f"📋 검색 결과 ({len(results.matches)}개):")
        print("-" * 60)
        
        for i, match in enumerate(results.matches, 1):
            print(f"\n{i}. 점수: {match.score:.4f}")
            print(f"   ID: {match.id}")
            if match.metadata:
                print(f"   출처: {match.metadata.get('source_file', 'N/A')}")
                print(f"   내용: {match.metadata.get('text', 'N/A')[:200]}...")
                
                # 다른 메타데이터도 표시
                for key, value in match.metadata.items():
                    if key not in ['text', 'source_file', 'row_index'] and value:
                        print(f"   {key}: {value}")
            
    except Exception as e:
        print(f"❌ 검색 실패: {e}")

# 실행
if __name__ == "__main__":
    print("📊 CSV 파일로 Pinecone 벡터 DB 구축")
    print("="*60)
    
    # API 키 설정
    PINECONE_API_KEY = "pcsk_5Wcu2A_7QAdTAjfmSYxwxc2sfiZ7G1bhmi9qy6J2KXL1hUfNcoLQ3xGdavA7S4E9DEqpmH"
    OPENAI_API_KEY = "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"
    
    # CSV 파일 경로
    CSV_FILES = [
        "D:/Github/ai-server/app/vectorDB/weekly_report_cloud3.csv",
        "D:/Github/ai-server/app/vectorDB/weekly_report_esg.csv"
    ]
    
    print(f"📁 처리할 CSV 파일:")
    for file_path in CSV_FILES:
        print(f"  - {file_path}")
    print("="*60)
    
    # 벡터 DB 구축 실행
    builder = CSVToPineconeBuilder(
        pinecone_api_key=PINECONE_API_KEY,
        openai_api_key=OPENAI_API_KEY
    )
    
    success, index_name, namespace = builder.run_pipeline(CSV_FILES)
    
    if success:
        print("\n" + "="*60)
        print("🔍 검색 테스트")
        print("="*60)
        
        # 검색 테스트
        test_queries = ["프로젝트", "개발", "클라우드", "ESG"]
        
        for query in test_queries:
            print(f"\n{'='*20} '{query}' 검색 {'='*20}")
            search_csv_data(
                query, 
                PINECONE_API_KEY, 
                OPENAI_API_KEY,
                index_name,
                namespace,
                top_k=3
            )
        
        print(f"\n🎉 완료!")
        print(f"📍 인덱스: {index_name}")
        print(f"📛 네임스페이스: {namespace}")
        print("💡 search_csv_data 함수로 검색 가능!")
    else:
        print("\n❌ 벡터 DB 구축 실패")
        print("💡 Pinecone 제한 또는 CSV 파일 문제일 수 있습니다.")