import pymysql
from openai import OpenAI
import chromadb
import numpy as np
import json
from typing import List, Dict, Any
import time
from tqdm import tqdm
import os

class RDBToChromaBuilder:
    def __init__(self, 
                 openai_api_key: str,
                 db_config: Dict[str, Any],
                 chroma_db_path: str = "./chroma_db"):
        """
        RDB 데이터를 Chroma 벡터 DB로 구축하는 클래스
        
        Args:
            openai_api_key: OpenAI API 키
            db_config: 데이터베이스 연결 설정
            chroma_db_path: Chroma DB 저장 경로
        """
        self.openai_api_key = openai_api_key
        self.db_config = db_config
        self.chroma_db_path = chroma_db_path
        
        # OpenAI 클라이언트 초기화
        self.openai_client = OpenAI(api_key=self.openai_api_key)
        
        # Chroma 클라이언트 초기화
        self.chroma_client = chromadb.PersistentClient(path=self.chroma_db_path)
        
        # 설정값
        self.embedding_model = "text-embedding-3-small"
        self.collection_name = "personal_weekly"
        
    def connect_to_db(self):
        """MariaDB 연결"""
        try:
            connection = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['username'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
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
                model=self.embedding_model,
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            print(f"❌ 임베딩 생성 실패: {e}")
            return None
    
    def setup_chroma_collection(self):
        """Chroma 컬렉션 설정"""
        try:
            # 기존 컬렉션이 있는지 확인
            try:
                self.collection = self.chroma_client.get_collection(name=self.collection_name)
                print(f"✅ 기존 컬렉션 '{self.collection_name}' 사용")
                
                # 기존 데이터 확인
                count = self.collection.count()
                print(f"📊 기존 벡터 수: {count}개")
                
                # 기존 컬렉션 삭제 여부 확인
                response = input("기존 컬렉션을 삭제하고 새로 만드시겠습니까? (y/N): ")
                if response.lower() == 'y':
                    self.chroma_client.delete_collection(name=self.collection_name)
                    print("🗑️ 기존 컬렉션 삭제 완료")
                    raise Exception("새 컬렉션 생성 필요")
                    
            except:
                # 새 컬렉션 생성
                self.collection = self.chroma_client.create_collection(
                    name=self.collection_name,
                    metadata={"description": "Weekly reports vector database"}
                )
                print(f"✅ 새 컬렉션 '{self.collection_name}' 생성 완료")
            
            return True
            
        except Exception as e:
            print(f"❌ Chroma 컬렉션 설정 실패: {e}")
            return False
    
    def add_to_chroma(self, records: List[Dict]):
        """Chroma에 벡터 데이터 추가"""
        documents = []
        embeddings = []
        metadatas = []
        ids = []
        
        print("🔄 임베딩 생성 및 Chroma 업로드 중...")
        
        batch_size = 100
        total_processed = 0
        
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
                'source_table': 'weekly_reports',
                'user_id': str(record.get('user_id', '')),
                'organization_id': str(record.get('organization_id', '')),
                'start_date': str(record.get('start_date', '')),
                'end_date': str(record.get('end_date', '')),
                'evaluation_year': str(record.get('evaluation_year', '')),
                'evaluation_quarter': str(record.get('evaluation_quarter', '')),
                'created_at': str(record.get('created_at', ''))
            }
            
            # 데이터 추가
            documents.append(text)
            embeddings.append(embedding)
            metadatas.append(metadata)
            ids.append(f"weekly_report_{record.get('id', i)}")
            
            # 배치 처리
            if len(documents) >= batch_size:
                try:
                    self.collection.add(
                        documents=documents,
                        embeddings=embeddings,
                        metadatas=metadatas,
                        ids=ids
                    )
                    total_processed += len(documents)
                    print(f"✅ {total_processed}개 벡터 업로드 완료")
                    
                    # 리스트 초기화
                    documents = []
                    embeddings = []
                    metadatas = []
                    ids = []
                    
                    time.sleep(0.1)  # API 제한 방지
                except Exception as e:
                    print(f"❌ 배치 업로드 실패: {e}")
        
        # 남은 데이터 업로드
        if documents:
            try:
                self.collection.add(
                    documents=documents,
                    embeddings=embeddings,
                    metadatas=metadatas,
                    ids=ids
                )
                total_processed += len(documents)
                print(f"✅ 총 {total_processed}개 벡터 업로드 완료")
            except Exception as e:
                print(f"❌ 최종 배치 업로드 실패: {e}")
    
    def get_collection_stats(self):
        """컬렉션 통계 확인"""
        try:
            count = self.collection.count()
            print("📊 Chroma 컬렉션 통계:")
            print(f"  - 총 벡터 수: {count}")
            print(f"  - 컬렉션명: {self.collection_name}")
            print(f"  - 저장 경로: {self.chroma_db_path}")
            return count
        except Exception as e:
            print(f"❌ 컬렉션 통계 조회 실패: {e}")
            return None
    
    def run_pipeline(self):
        """전체 파이프라인 실행"""
        print("🚀 RDB to Chroma 벡터 DB 구축 시작")
        
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
            
            # 3. Chroma 컬렉션 설정
            if not self.setup_chroma_collection():
                return False
            
            # 4. 임베딩 및 업로드
            self.add_to_chroma(records)
            
            # 5. 결과 확인
            self.get_collection_stats()
            
            print("✅ 벡터 DB 구축 완료!")
            return True
            
        finally:
            connection.close()
            print("🔌 데이터베이스 연결 종료")

# 검색 예시 함수
def search_vectors(query: str, openai_api_key: str, chroma_db_path: str = "./chroma_db", top_k: int = 5):
    """벡터 검색 예시"""
    # OpenAI로 쿼리 임베딩
    client = OpenAI(api_key=openai_api_key)
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=query
    )
    query_embedding = response.data[0].embedding
    
    # Chroma 검색
    chroma_client = chromadb.PersistentClient(path=chroma_db_path)
    collection = chroma_client.get_collection(name="personal_weekly")
    
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
        include=['documents', 'metadatas', 'distances']
    )
    
    print(f"🔍 '{query}' 검색 결과:")
    for i, (doc, metadata, distance) in enumerate(zip(
        results['documents'][0], 
        results['metadatas'][0], 
        results['distances'][0]
    ), 1):
        print(f"\n{i}. 유사도: {1-distance:.4f}")
        print(f"   사용자 ID: {metadata.get('user_id', 'N/A')}")
        print(f"   기간: {metadata.get('start_date', 'N/A')} ~ {metadata.get('end_date', 'N/A')}")
        print(f"   내용: {doc[:200]}...")

def init_resources(openai_api_key: str, chroma_db_path: str = "./chroma_db"):
    """필요한 리소스 초기화"""
    # OpenAI 설정
    openai_client = OpenAI(api_key=openai_api_key)
    
    # Chroma 클라이언트 초기화
    chroma_client = chromadb.PersistentClient(path=chroma_db_path)
    collection = chroma_client.get_collection(name="personal_weekly")
    
    print(f"✅ 리소스 초기화 완료. 컬렉션: personal_weekly")
    
    return openai_client, collection

# 사용 예시
if __name__ == "__main__":
    # 설정값
    OPENAI_API_KEY = "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"
    
    DB_CONFIG = {
        'host': '13.209.110.151',
        'port': 3306,
        'username': 'root',
        'password': 'root',
        'database': 'skala'
    }
    
    # 벡터 DB 구축 실행
    builder = RDBToChromaBuilder(
        openai_api_key=OPENAI_API_KEY,
        db_config=DB_CONFIG,
        chroma_db_path="./skore_chroma_db"  # 원하는 경로로 설정
    )
    
    builder.run_pipeline()
    
    # 검색 테스트
    print("\n" + "="*50)
    print("검색 테스트")
    search_vectors("프로젝트 개발", OPENAI_API_KEY, "./skore_chroma_db")