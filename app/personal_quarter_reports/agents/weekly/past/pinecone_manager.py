from pinecone import Pinecone
import random

class PineconeManager:
    def __init__(self, api_key, index_name):
        self.pc = Pinecone(api_key=api_key)
        self.index_name = index_name
        self.index = self.pc.Index(index_name)
        self.namespace = self._detect_namespace()
        
        logger.info(f"Pinecone 초기화 완료 - 인덱스: {index_name}")
        logger.info(f"사용 네임스페이스: {self.namespace}")
    
    def _detect_namespace(self):
        """네임스페이스 자동 감지"""
        try:
            stats = self.index.describe_index_stats()
            if hasattr(stats, 'namespaces') and stats.namespaces:
                namespaces = list(stats.namespaces.keys())
                if namespaces:
                    for ns in namespaces:
                        if stats.namespaces[ns].vector_count > 0:
                            logger.info(f"네임스페이스 감지: '{ns}' (벡터 수: {stats.namespaces[ns].vector_count})")
                            return ns
                    return namespaces[0]
            return ""
        except Exception as e:
            logger.warning(f"네임스페이스 감지 실패: {e}")
            return ""
    
    def search_user_data(self, user_id, top_k=100):
        """특정 사용자 데이터 검색"""
        logger.info(f"사용자 {user_id} 데이터 검색 시작")
        
        dummy_vector = [0.0] * 1024
        
        query_params = {
            "vector": dummy_vector,
            "filter": {"user_id": str(user_id)},
            "top_k": top_k,
            "include_metadata": True
        }
        
        if self.namespace:
            query_params["namespace"] = self.namespace
        
        search_results = self.index.query(**query_params)
        logger.info(f"검색 결과: {len(search_results.matches)}건")
        
        return search_results
    
    def get_available_user_ids(self):
        """사용 가능한 모든 user_id 조회"""
        logger.info("사용 가능한 user_id 조회 시작")
        
        user_ids = set()
        dummy_vector = [0.0] * 1024
        
        # 여러 번 시도하여 더 많은 데이터 수집
        for attempt in range(3):
            try:
                if attempt > 0:
                    dummy_vector = [random.uniform(-1, 1) for _ in range(1024)]
                
                query_params = {
                    "vector": dummy_vector,
                    "top_k": 1000,
                    "include_metadata": True
                }
                
                if self.namespace:
                    query_params["namespace"] = self.namespace
                
                query_result = self.index.query(**query_params)
                
                for match in query_result.matches:
                    if hasattr(match, 'metadata') and match.metadata:
                        for key in ['user_id', 'userId', 'USER_ID', 'employee_id', 'emp_id']:
                            if key in match.metadata:
                                user_ids.add(str(match.metadata[key]))
                
                if user_ids:
                    break
                    
            except Exception as e:
                logger.warning(f"시도 {attempt + 1} 실패: {e}")
                continue
        
        available_ids = sorted(list(user_ids))
        logger.info(f"발견된 user_id: {available_ids}")
        return available_ids
    
    def validate_connection(self, user_id=None):
        """연결 상태 및 데이터 존재 여부 검증"""
        try:
            index_stats = self.index.describe_index_stats()
            
            result = {
                "index_name": self.index_name,
                "total_vectors": index_stats.total_vector_count,
                "namespace": self.namespace,
                "status": "connected"
            }
            
            if user_id:
                search_result = self.search_user_data(user_id, top_k=1)
                result["user_data_found"] = len(search_result.matches) > 0
            
            return result
            
        except Exception as e:
            logger.error(f"Pinecone 검증 실패: {e}")
            raise