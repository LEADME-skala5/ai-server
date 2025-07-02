import pymysql
from openai import OpenAI
import pinecone
import numpy as np
import json
from typing import List, Dict, Any
import time
from tqdm import tqdm
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

class RDBToPineconeBuilder:
    def __init__(self, 
                 pinecone_api_key: str,
                 openai_api_key: str,
                 db_config: Dict[str, Any],
                 index_name: str = None,
                 namespace: str = "personal_weekly"):
        """
        RDB 데이터를 Pinecone 벡터 DB로 구축하는 클래스
        
        Args:
            pinecone_api_key: Pinecone API 키
            openai_api_key: OpenAI API 키
            db_config: 데이터베이스 연결 설정
            index_name: Pinecone 인덱스 이름
            namespace: Pinecone 네임스페이스
        """
        self.pinecone_api_key = pinecone_api_key
        self.openai_api_key = openai_api_key
        self.db_config = db_config
        
        # OpenAI 클라이언트 초기화
        self.openai_client = OpenAI(api_key=self.openai_api_key)
        
        # Pinecone 초기화 시도 (여러 환경 테스트)
        self.pinecone_initialized = False
        environments_to_try = [
            None,  # 최신 버전에서는 environment 불필요할 수 있음
            "us-east1-gcp",
            "us-west1-gcp",
            "asia-northeast1-gcp",
            "europe-west1-gcp"
        ]
        
        for env in environments_to_try:
            try:
                if env is None:
                    # 최신 버전 방식 시도
                    pinecone.init(api_key=self.pinecone_api_key)
                else:
                    # 구버전 방식 시도
                    pinecone.init(api_key=self.pinecone_api_key, environment=env)
                
                self.pinecone_initialized = True
                print(f"✅ Pinecone 초기화 성공" + (f" (환경: {env})" if env else ""))
                break
            except Exception as e:
                continue
        
        if not self.pinecone_initialized:
            raise Exception("❌ Pinecone 초기화 실패. API 키 또는 환경 설정을 확인하세요.")
        
        # 설정값 (환경변수에서 가져오거나 기본값 사용)
        self.embedding_model = os.getenv('OPENAI_MODEL', 'gpt-4-turbo')  # 임베딩 모델은 별도 설정 필요
        self.dimension = 1536  # text-embedding-3-small 기본 차원
        self.index_name = index_name or os.getenv('PINECONE_INDEX_NAME', 'skore')
        self.namespace = namespace
        
    def connect_to_db(self):
        """MariaDB 연결"""
        try:
            connection = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['username'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset=self.db_config.get('charset', 'utf8mb4'),
                cursorclass=pymysql.cursors.DictCursor
            )
            print("✅ 데이터베이스 연결 성공")
            return connection
        except Exception as e:
            print(f"❌ 데이터베이스 연결 실패: {e}")
            return None
    
    def fetch_weekly_reports(self, connection) -> List[Dict]:
        """weekly_reports 테이블에서 데이터 조회"""
        try:
            with connection.cursor() as cursor:
                # 테이블 구조 확인
                cursor.execute("DESCRIBE weekly_reports")
                columns = cursor.fetchall()
                print("📋 테이블 구조:")
                for col in columns:
                    print(f"  - {col['Field']}: {col['Type']}")
                
                # 전체 데이터 조회
                cursor.execute("SELECT * FROM weekly_reports")
                results = cursor.fetchall()
                print(f"📊 총 {len(results)}개의 레코드를 가져왔습니다.")
                return results
                
        except Exception as e:
            print(f"❌ 데이터 조회 실패: {e}")
            return []
    
    def create_embedding_text(self, record: Dict) -> str:
        """
        레코드를 임베딩할 텍스트로 변환
        weekly_reports 테이블의 필드에 맞게 최적화
        """
        text_parts = []
        
        # done_task가 메인 내용이므로 우선적으로 포함
        if record.get('done_task'):
            text_parts.append(f"완료 업무: {record['done_task']}")
        
        # 사용자 ID와 조직 ID 추가
        if record.get('user_id'):
            text_parts.append(f"사용자 ID: {record['user_id']}")
        
        if record.get('organization_id'):
            text_parts.append(f"조직 ID: {record['organization_id']}")
        
        # 기간 정보 추가
        if record.get('start_date') and record.get('end_date'):
            text_parts.append(f"기간: {record['start_date']} ~ {record['end_date']}")
        
        # 평가 정보 추가
        if record.get('evaluation_year') and record.get('evaluation_quarter'):
            text_parts.append(f"평가: {record['evaluation_year']}년 {record['evaluation_quarter']}분기")
        
        return " | ".join(text_parts)
    
    def get_embedding(self, text: str) -> List[float]:
        """OpenAI API를 사용해 텍스트 임베딩 생성"""
        try:
            response = self.openai_client.embeddings.create(
                model="text-embedding-3-small",  # 임베딩 전용 모델 사용
                input=text
                # dimensions 파라미터 제거 (기본 1536 차원 사용)
            )
            return response.data[0].embedding
        except Exception as e:
            print(f"❌ 임베딩 생성 실패: {e}")
            return None
    
    def setup_pinecone_index(self):
        """Pinecone 인덱스 설정"""
        try:
            # 기존 인덱스 확인
            existing_indexes = pinecone.list_indexes()
            
            if self.index_name not in existing_indexes:
                # 인덱스 생성 (구버전 방식)
                pinecone.create_index(
                    name=self.index_name,
                    dimension=self.dimension,
                    metric='cosine'
                )
                print(f"✅ 인덱스 '{self.index_name}' 생성 완료")
                
                # 인덱스 초기화 대기
                time.sleep(60)
            else:
                print(f"✅ 인덱스 '{self.index_name}' 이미 존재함")
            
            # 인덱스 연결
            self.index = pinecone.Index(self.index_name)
            return True
            
        except Exception as e:
            print(f"❌ Pinecone 인덱스 설정 실패: {e}")
            return False
    
    def upsert_to_pinecone(self, records: List[Dict]):
        """Pinecone에 벡터 데이터 업서트"""
        vectors = []
        
        print("🔄 임베딩 생성 및 Pinecone 업로드 중...")
        
        for i, record in enumerate(tqdm(records, desc="처리 중")):
            # 임베딩할 텍스트 생성
            text = self.create_embedding_text(record)
            if not text.strip():
                continue
            
            # 임베딩 생성
            embedding = self.get_embedding(text)
            if embedding is None:
                continue
            
            # 메타데이터 준비
            metadata = {
                'text': text,
                'source_table': 'weekly_reports',
                'namespace': self.namespace,
                'user_id': str(record.get('user_id', '')),
                'organization_id': str(record.get('organization_id', '')),
                'start_date': str(record.get('start_date', '')),
                'end_date': str(record.get('end_date', '')),
                'evaluation_year': str(record.get('evaluation_year', '')),
                'evaluation_quarter': str(record.get('evaluation_quarter', ''))
            }
            
            # 벡터 데이터 준비
            vector_data = {
                'id': f"weekly_report_{record.get('id', i)}",
                'values': embedding,
                'metadata': metadata
            }
            
            vectors.append(vector_data)
            
            # 배치 처리 (100개씩)
            if len(vectors) >= 100:
                try:
                    self.index.upsert(
                        vectors=vectors,
                        namespace=self.namespace
                    )
                    vectors = []
                    time.sleep(1)  # API 제한 방지
                except Exception as e:
                    print(f"❌ 배치 업로드 실패: {e}")
        
        # 남은 벡터들 업로드
        if vectors:
            try:
                self.index.upsert(
                    vectors=vectors,
                    namespace=self.namespace
                )
                print("✅ 모든 벡터 업로드 완료")
            except Exception as e:
                print(f"❌ 최종 배치 업로드 실패: {e}")
    
    def get_index_stats(self):
        """인덱스 통계 확인"""
        try:
            stats = self.index.describe_index_stats()
            print("📊 Pinecone 인덱스 통계:")
            print(f"  - 전체 벡터 수: {stats.total_vector_count}")
            print(f"  - 네임스페이스별 통계: {stats.namespaces}")
            return stats
        except Exception as e:
            print(f"❌ 인덱스 통계 조회 실패: {e}")
            return None
    
    def run_pipeline(self):
        """전체 파이프라인 실행"""
        print("🚀 RDB to Pinecone 벡터 DB 구축 시작")
        
        # 1. DB 연결
        connection = self.connect_to_db()
        if not connection:
            return False
        
        try:
            # 2. 데이터 조회
            records = self.fetch_weekly_reports(connection)
            if not records:
                print("❌ 처리할 데이터가 없습니다.")
                return False
            
            # 3. Pinecone 인덱스 설정
            if not self.setup_pinecone_index():
                return False
            
            # 4. 임베딩 및 업로드
            self.upsert_to_pinecone(records)
            
            # 5. 결과 확인
            self.get_index_stats()
            
            print("✅ 벡터 DB 구축 완료!")
            return True
            
        finally:
            connection.close()
            print("🔌 데이터베이스 연결 종료")

# 검색 예시 함수
def search_vectors(query: str, top_k: int = 5):
    """벡터 검색 예시 - 환경변수 사용"""
    pinecone_api_key = os.getenv('PINECONE_API_KEY')
    openai_api_key = os.getenv('OPENAI_API_KEY')
    index_name = os.getenv('PINECONE_INDEX_NAME', 'skore')
    
    if not pinecone_api_key or not openai_api_key:
        print("❌ 필요한 API 키가 환경변수에 설정되지 않았습니다.")
        return
    
    # OpenAI로 쿼리 임베딩
    client = OpenAI(api_key=openai_api_key)
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=query
    )
    query_embedding = response.data[0].embedding
    
    # Pinecone 검색 (구버전 방식)
    pinecone.init(api_key=pinecone_api_key)
    index = pinecone.Index(index_name)
    
    results = index.query(
        vector=query_embedding,
        top_k=top_k,
        namespace="personal_weekly",
        include_metadata=True
    )
    
    print(f"🔍 '{query}' 검색 결과:")
    for i, match in enumerate(results.matches, 1):
        print(f"\n{i}. 점수: {match.score:.4f}")
        print(f"   ID: {match.id}")
        print(f"   텍스트: {match.metadata.get('text', 'N/A')[:200]}...")

def init_resources(index_name: str = None):
    """필요한 리소스 초기화 - 환경변수 사용"""
    pinecone_api_key = os.getenv('PINECONE_API_KEY')
    openai_api_key = os.getenv('OPENAI_API_KEY')
    index_name = index_name or os.getenv('PINECONE_INDEX_NAME', 'skore')
    
    if not pinecone_api_key or not openai_api_key:
        print("❌ 필요한 API 키가 환경변수에 설정되지 않았습니다.")
        return None, None
    
    # OpenAI 설정
    openai_client = OpenAI(api_key=openai_api_key)
    
    # Pinecone 클라이언트 초기화 (구버전)
    pinecone.init(api_key=pinecone_api_key)
    pinecone_index = pinecone.Index(index_name)
    
    print(f"✅ 리소스 초기화 완료. 인덱스: {index_name}")
    
    return openai_client, pinecone_index

def load_db_config_from_env():
    """환경변수에서 DB 설정을 로드"""
    return {
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'username': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME'),
        'charset': os.getenv('DB_CHARSET', 'utf8mb4')
    }

# 사용 예시
if __name__ == "__main__":
    # 환경변수에서 설정값 로드
    PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
    
    # 필수 환경변수 확인
    if not PINECONE_API_KEY or not OPENAI_API_KEY:
        print("❌ 필수 환경변수가 설정되지 않았습니다:")
        print("   - PINECONE_API_KEY")
        print("   - OPENAI_API_KEY")
        print("   .env 파일을 확인하세요.")
        exit(1)
    
    # DB 설정 로드
    DB_CONFIG = load_db_config_from_env()
    
    # DB 설정 유효성 검사
    required_db_fields = ['host', 'username', 'password', 'database']
    missing_fields = [field for field in required_db_fields if not DB_CONFIG.get(field)]
    
    if missing_fields:
        print(f"❌ 누락된 DB 설정: {', '.join(missing_fields)}")
        print("   .env 파일의 DB 관련 환경변수를 확인하세요.")
        exit(1)
    
    print("✅ 모든 환경변수가 정상적으로 로드되었습니다.")
    
    # 벡터 DB 구축 실행
    builder = RDBToPineconeBuilder(
        pinecone_api_key=PINECONE_API_KEY,
        openai_api_key=OPENAI_API_KEY,
        db_config=DB_CONFIG
    )
    
    builder.run_pipeline()