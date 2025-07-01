import pandas as pd
import json
import openai
from typing import Dict, List, Any, Optional, Tuple
import os
from datetime import datetime
from pathlib import Path
import logging
import pymysql
from pinecone import Pinecone, ServerlessSpec
import random

# .env 파일 지원 (선택사항)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv가 설치되지 않은 경우 무시

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeeklyReportEvaluationAgent:
    def __init__(self, 
                openai_api_key: Optional[str] = None,
                pinecone_api_key: Optional[str] = None,
                model: Optional[str] = None,
                output_path: Optional[str] = None):
        """
        AI 기반 주간 보고서 평가 에이전트 (Pinecone + MariaDB 버전)
        
        Args:
            openai_api_key: OpenAI API 키 (기본값: 환경변수에서 가져옴)
            pinecone_api_key: Pinecone API 키 (기본값: 환경변수에서 가져옴)
            model: 사용할 LLM 모델명 (기본값: 환경변수에서 가져옴)
            output_path: 결과 파일들을 저장할 경로 (기본값: 환경변수에서 가져옴)
        """
        # OpenAI API 키 설정
        final_openai_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        if not final_openai_key:
            raise ValueError("OpenAI API 키가 설정되지 않았습니다.")
        
        # Pinecone API 키 설정
        final_pinecone_key = pinecone_api_key or os.getenv("PINECONE_API_KEY")
        if not final_pinecone_key:
            raise ValueError("Pinecone API 키가 설정되지 않았습니다.")
        
        # 모델 설정 (환경변수에서 기본값 가져오기)
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4-turbo")
        
        # 출력 경로 설정 (환경변수에서 기본값 가져오기)
        output_path = output_path or os.getenv("OUTPUT_PATH", "./output")
        
        self.openai_client = openai.OpenAI(api_key=final_openai_key)
        self.output_path = Path(output_path)
        
        # Pinecone 초기화
        self.pc = Pinecone(api_key=final_pinecone_key)
        self.pinecone_index_name = os.getenv("PINECONE_INDEX_NAME", "skore-20250624-144422")
        self.index = self.pc.Index(self.pinecone_index_name)
        
        # 네임스페이스 자동 감지
        self.namespace = self._detect_namespace()
        
        # MariaDB 연결 정보 (환경변수에서 가져오기)
        self.db_config = {
            'host': os.getenv("DB_HOST", '13.209.110.151'),
            'port': int(os.getenv("DB_PORT", 27017)),
            'user': os.getenv("DB_USER", 'root'),
            'password': os.getenv("DB_PASSWORD", 'root'),
            'database': os.getenv("DB_DATABASE", 'skala'),
            'charset': os.getenv("DB_CHARSET", 'utf8mb4')
        }
        
        # 데이터 저장소
        self.weekly_data = None
        self.team_criteria = None
        self.team_goals = None
        
        # 에이전트 상태 추적
        self.evaluation_history = []
        self.current_context = {}
        
        # 출력 디렉토리 생성
        self.output_path.mkdir(exist_ok=True)
        
        logger.info(f"WeeklyReportEvaluationAgent 초기화 완료 - 모델: {self.model}")
        logger.info(f"Pinecone 인덱스: {self.pinecone_index_name}")
        logger.info(f"사용 네임스페이스: {self.namespace}")
        logger.info(f"데이터베이스: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
    
    def _detect_namespace(self) -> str:
        """Pinecone 네임스페이스를 자동 감지합니다."""
        try:
            stats = self.index.describe_index_stats()
            if hasattr(stats, 'namespaces') and stats.namespaces:
                namespaces = list(stats.namespaces.keys())
                if namespaces:
                    # 데이터가 있는 첫 번째 네임스페이스 사용
                    for ns in namespaces:
                        if stats.namespaces[ns].vector_count > 0:
                            logger.info(f"네임스페이스 감지: '{ns}' (벡터 수: {stats.namespaces[ns].vector_count})")
                            return ns
                    return namespaces[0]
            return ""
        except Exception as e:
            logger.warning(f"네임스페이스 감지 실패: {e}")
            return ""
        
    def connect_to_mariadb(self):
        """MariaDB에 연결합니다."""
        try:
            connection = pymysql.connect(**self.db_config)
            logger.info("MariaDB 연결 성공")
            return connection
        except Exception as e:
            logger.error(f"MariaDB 연결 실패: {str(e)}")
            raise ValueError(f"데이터베이스 연결 중 오류 발생: {str(e)}")
    
    def load_team_data_from_rdb(self) -> Dict[str, Any]:
        """MariaDB에서 팀 기준 및 목표 데이터를 로드합니다."""
        logger.info("MariaDB에서 팀 데이터 로드 시작")
        
        try:
            connection = self.connect_to_mariadb()
            
            # team_criteria 테이블 로드
            criteria_query = "SELECT * FROM team_criteria"
            self.team_criteria = pd.read_sql(criteria_query, connection)
            logger.info(f"team_criteria 로드 완료: {len(self.team_criteria)}건")
            
            # team_goal 테이블 로드
            goals_query = "SELECT * FROM team_goal"
            self.team_goals = pd.read_sql(goals_query, connection)
            logger.info(f"team_goal 로드 완료: {len(self.team_goals)}건")
            
            connection.close()
            
            return {
                "team_criteria_records": len(self.team_criteria),
                "team_goals_records": len(self.team_goals),
                "teams_available": self._extract_teams_from_rdb_data()
            }
            
        except Exception as e:
            logger.error(f"팀 데이터 로드 실패: {str(e)}")
            raise ValueError(f"팀 데이터 로드 중 오류 발생: {str(e)}")
    
    def load_weekly_data_from_pinecone(self, user_id: str) -> Dict[str, Any]:
        """Pinecone에서 특정 사용자의 주간 데이터를 검색합니다."""
        logger.info(f"Pinecone에서 사용자 {user_id} 데이터 검색 시작")
        
        try:
            # 사용자 ID 기반 검색 쿼리 생성
            # 더미 벡터로 검색 (실제로는 메타데이터 필터링만 사용)
            dummy_vector = [0.0] * 1024
            
            # 검색 파라미터 설정
            query_params = {
                "vector": dummy_vector,
                "filter": {"user_id": str(user_id)},  # 명시적으로 문자열 변환
                "top_k": 100,  # 충분히 큰 수로 설정
                "include_metadata": True
            }
            
            # 네임스페이스가 있으면 추가
            if self.namespace:
                query_params["namespace"] = self.namespace
            
            search_results = self.index.query(**query_params)
            
            logger.info(f"Pinecone 검색 결과: {len(search_results.matches)}건")
            
            if not search_results.matches:
                raise ValueError(f"사용자 ID {user_id}에 해당하는 데이터가 Pinecone에 없습니다.")
            
            # 검색 결과를 DataFrame 형태로 변환
            weekly_records = []
            reference_data = []
            
            for match in search_results.matches:
                metadata = match.metadata
                
                # weekly.csv와 동일한 구조로 변환
                record = {
                    'employee_number': metadata.get('user_id'),
                    'name': f"User_{metadata.get('user_id')}",  # 실제 이름은 별도 테이블에서 가져올 수 있음
                    'done_task': metadata.get('done_task', ''),
                    'start_date': metadata.get('start_date'),
                    'end_date': metadata.get('end_date'),
                    'evaluation_year': metadata.get('evaluation_year'),
                    'evaluation_quarter': metadata.get('evaluation_quarter'),
                    'organization_id': metadata.get('organization_id'),
                    'source_file': metadata.get('source_file', ''),
                    'row_index': metadata.get('row_index', '')
                }
                
                weekly_records.append(record)
                
                # reference 정보 수집
                reference_data.append({
                    'id': match.id,
                    'score': match.score,
                    'metadata': metadata,
                    'text_preview': metadata.get('text', '')[:200] + '...' if metadata.get('text') else ''
                })
            
            # DataFrame으로 변환
            self.weekly_data = pd.DataFrame(weekly_records)
            
            # 중복 제거 (같은 주차 데이터가 중복될 수 있음)
            self.weekly_data = self.weekly_data.drop_duplicates(
                subset=['employee_number', 'start_date', 'end_date'], 
                keep='first'
            )
            
            logger.info(f"주간 데이터 로드 완료: {len(self.weekly_data)}건 (중복 제거 후)")
            
            return {
                "weekly_records": len(self.weekly_data),
                "pinecone_matches": len(search_results.matches),
                "date_range": self._extract_date_range(),
                "reference_data": reference_data
            }
            
        except Exception as e:
            logger.error(f"Pinecone 데이터 로드 실패: {str(e)}")
            raise ValueError(f"Pinecone 데이터 로드 중 오류 발생: {str(e)}")
    
    def plan_evaluation(self, 
                       target_user_id: str,
                       target_employees: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        평가 계획을 수립합니다.
        """
        logger.info("=== 평가 계획 수립 시작 ===")
        
        plan = {
            "timestamp": datetime.now().isoformat(),
            "data_sources": {
                "weekly_data": "Pinecone Vector DB",
                "team_criteria": "MariaDB - team_criteria 테이블",
                "team_goals": "MariaDB - team_goal 테이블"
            },
            "target_user_id": target_user_id,
            "target_employees": target_employees,
            "steps": []
        }
        
        # 1단계: 데이터 소스 연결 검증
        plan["steps"].append({
            "step": 1,
            "action": "Pinecone 및 MariaDB 연결 검증",
            "status": "planned"
        })
        
        # 2단계: 데이터 로드
        plan["steps"].append({
            "step": 2,
            "action": "Pinecone에서 주간 데이터, MariaDB에서 팀 데이터 로드",
            "status": "planned"
        })
        
        # 3단계: 데이터 전처리 및 검증
        plan["steps"].append({
            "step": 3,
            "action": "데이터 무결성 검사 및 전처리",
            "status": "planned"
        })
        
        # 4단계: 평가 실행
        plan["steps"].append({
            "step": 4,
            "action": "AI 기반 개별 직원 평가 수행",
            "status": "planned"
        })
        
        # 5단계: 결과 저장 (reference 포함)
        plan["steps"].append({
            "step": 5,
            "action": "평가 결과 및 참조 정보 저장",
            "status": "planned"
        })
        
        self.current_context["plan"] = plan
        logger.info(f"평가 계획 수립 완료 - {len(plan['steps'])}단계")
        
        return plan
    
    def validate_data_sources(self, user_id: str) -> Dict[str, Any]:
        """
        데이터 소스들의 연결 상태와 데이터 존재 여부를 검증합니다.
        """
        logger.info("데이터 소스 검증 시작")
        
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "source_info": {}
        }
        
        # Pinecone 연결 및 데이터 확인
        try:
            # 인덱스 정보 확인
            index_stats = self.index.describe_index_stats()
            validation_result["source_info"]["pinecone"] = {
                "index_name": self.pinecone_index_name,
                "total_vectors": index_stats.total_vector_count,
                "dimension": 1024,
                "namespace": self.namespace,
                "status": "connected"
            }
            
            # 특정 사용자 데이터 존재 확인
            dummy_vector = [0.0] * 1024
            
            # 쿼리 파라미터 설정
            query_params = {
                "vector": dummy_vector,
                "filter": {"user_id": str(user_id)},  # 문자열로 변환
                "top_k": 1,
                "include_metadata": True
            }
            
            # 네임스페이스가 있으면 추가
            if self.namespace:
                query_params["namespace"] = self.namespace
            
            test_search = self.index.query(**query_params)
            
            if not test_search.matches:
                validation_result["errors"].append(f"사용자 ID {user_id}에 해당하는 데이터가 Pinecone에 없습니다.")
                validation_result["valid"] = False
            else:
                validation_result["source_info"]["pinecone"]["user_data_found"] = True
                
        except Exception as e:
            validation_result["errors"].append(f"Pinecone 연결 오류: {str(e)}")
            validation_result["valid"] = False
        
        # MariaDB 연결 및 테이블 확인
        try:
            connection = self.connect_to_mariadb()
            cursor = connection.cursor()
            
            # 테이블 존재 확인
            cursor.execute("SHOW TABLES LIKE 'team_criteria'")
            if not cursor.fetchone():
                validation_result["errors"].append("team_criteria 테이블이 존재하지 않습니다.")
                validation_result["valid"] = False
            
            cursor.execute("SHOW TABLES LIKE 'team_goal'")
            if not cursor.fetchone():
                validation_result["errors"].append("team_goal 테이블이 존재하지 않습니다.")
                validation_result["valid"] = False
            
            # 테이블 레코드 수 확인
            cursor.execute("SELECT COUNT(*) FROM team_criteria")
            criteria_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM team_goal")
            goals_count = cursor.fetchone()[0]
            
            validation_result["source_info"]["mariadb"] = {
                "host": self.db_config["host"],
                "database": self.db_config["database"],
                "team_criteria_records": criteria_count,
                "team_goals_records": goals_count,
                "status": "connected"
            }
            
            connection.close()
            
        except Exception as e:
            validation_result["errors"].append(f"MariaDB 연결 오류: {str(e)}")
            validation_result["valid"] = False
        
        if validation_result["valid"]:
            logger.info("모든 데이터 소스 검증 성공")
        else:
            logger.error(f"데이터 소스 검증 실패: {validation_result['errors']}")
            
        return validation_result.info("데이터 소스 검증 시작")
        
    def validate_data_sources(self, user_id: str) -> Dict[str, Any]:
        """
        데이터 소스들의 연결 상태와 데이터 존재 여부를 검증합니다.
        """
        logger.info("데이터 소스 검증 시작")
        
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "source_info": {}
        }
        
        # Pinecone 연결 및 데이터 확인
        try:
            # 인덱스 정보 확인
            index_stats = self.index.describe_index_stats()
            validation_result["source_info"]["pinecone"] = {
                "index_name": self.pinecone_index_name,
                "total_vectors": index_stats.total_vector_count,
                "dimension": 1024,
                "namespace": self.namespace,
                "status": "connected"
            }
            
            # 특정 사용자 데이터 존재 확인
            dummy_vector = [0.0] * 1024
            
            # 쿼리 파라미터 설정
            query_params = {
                "vector": dummy_vector,
                "filter": {"user_id": str(user_id)},  # 문자열로 변환
                "top_k": 1,
                "include_metadata": True
            }
            
            # 네임스페이스가 있으면 추가
            if self.namespace:
                query_params["namespace"] = self.namespace
            
            test_search = self.index.query(**query_params)
            
            if not test_search.matches:
                validation_result["errors"].append(f"사용자 ID {user_id}에 해당하는 데이터가 Pinecone에 없습니다.")
                validation_result["valid"] = False
            else:
                validation_result["source_info"]["pinecone"]["user_data_found"] = True
                
        except Exception as e:
            validation_result["errors"].append(f"Pinecone 연결 오류: {str(e)}")
            validation_result["valid"] = False
        
        # MariaDB 연결 및 테이블 확인
        try:
            connection = self.connect_to_mariadb()
            cursor = connection.cursor()
            
            # 테이블 존재 확인
            cursor.execute("SHOW TABLES LIKE 'team_criteria'")
            if not cursor.fetchone():
                validation_result["errors"].append("team_criteria 테이블이 존재하지 않습니다.")
                validation_result["valid"] = False
            
            cursor.execute("SHOW TABLES LIKE 'team_goal'")
            if not cursor.fetchone():
                validation_result["errors"].append("team_goal 테이블이 존재하지 않습니다.")
                validation_result["valid"] = False
            
            # 테이블 레코드 수 확인
            cursor.execute("SELECT COUNT(*) FROM team_criteria")
            criteria_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM team_goal")
            goals_count = cursor.fetchone()[0]
            
            validation_result["source_info"]["mariadb"] = {
                "host": self.db_config["host"],
                "database": self.db_config["database"],
                "team_criteria_records": criteria_count,
                "team_goals_records": goals_count,
                "status": "connected"
            }
            
            connection.close()
            
        except Exception as e:
            validation_result["errors"].append(f"MariaDB 연결 오류: {str(e)}")
            validation_result["valid"] = False
        
        if validation_result["valid"]:
            logger.info("모든 데이터 소스 검증 성공")
        else:
            logger.error(f"데이터 소스 검증 실패: {validation_result['errors']}")
            
        return validation_result
    
    def load_and_preprocess_data(self, user_id: str) -> Dict[str, Any]:
        """
        Pinecone과 MariaDB에서 데이터를 로드하고 전처리합니다.
        """
        logger.info("데이터 로드 및 전처리 시작")
        
        try:
            # 1. MariaDB에서 팀 데이터 로드
            team_data_result = self.load_team_data_from_rdb()
            
            # 2. Pinecone에서 주간 데이터 로드
            weekly_data_result = self.load_weekly_data_from_pinecone(user_id)
            
            # 전처리 결과 통합
            preprocessing_result = {
                "weekly_records": weekly_data_result["weekly_records"],
                "pinecone_matches": weekly_data_result["pinecone_matches"],
                "team_criteria_records": team_data_result["team_criteria_records"],
                "team_goals_records": team_data_result["team_goals_records"],
                "unique_employees": self.weekly_data['employee_number'].nunique() if self.weekly_data is not None else 0,
                "date_range": weekly_data_result["date_range"],
                "reference_data": weekly_data_result["reference_data"],
                "teams_available": team_data_result["teams_available"]
            }
            
            logger.info(f"데이터 로드 완료 - 주간 기록: {preprocessing_result['weekly_records']}건, "
                       f"팀 기준: {preprocessing_result['team_criteria_records']}건, "
                       f"팀 목표: {preprocessing_result['team_goals_records']}건")
            return preprocessing_result
            
        except Exception as e:
            logger.error(f"데이터 로드 실패: {str(e)}")
            raise ValueError(f"데이터 로드 중 오류 발생: {str(e)}")
    
    def _extract_teams_from_rdb_data(self) -> List[str]:
        """RDB 데이터에서 팀 목록을 추출합니다."""
        teams = set()
        
        # team_goals 데이터에서 팀 추출
        if self.team_goals is not None:
            team_column = self._find_column_by_keywords(
                self.team_goals, 
                ['team', 'org', 'group', 'dept', '팀', '조직', '부서', 'organization']
            )
            if team_column:
                teams.update(self.team_goals[team_column].dropna().unique())
        
        # team_criteria 데이터에서 팀 추출
        if self.team_criteria is not None:
            team_column = self._find_column_by_keywords(
                self.team_criteria, 
                ['team', 'org', 'group', 'dept', '팀', '조직', '부서', 'organization']
            )
            if team_column:
                teams.update(self.team_criteria[team_column].dropna().unique())
        
        return sorted(list(teams))
    
    def _extract_date_range(self) -> Dict[str, str]:
        """데이터의 날짜 범위를 추출합니다."""
        if self.weekly_data is None:
            return {}
            
        date_info = {}
        for date_col in ['start_date', 'end_date', 'date']:
            if date_col in self.weekly_data.columns:
                dates = pd.to_datetime(self.weekly_data[date_col], errors='coerce').dropna()
                if not dates.empty:
                    date_info[f"{date_col}_min"] = dates.min().strftime('%Y-%m-%d')
                    date_info[f"{date_col}_max"] = dates.max().strftime('%Y-%m-%d')
        
        return date_info
    
    def analyze_employee_data(self, employee_number: str) -> Dict[str, Any]:
        """
        특정 직원의 데이터를 분석합니다.
        """
        logger.info(f"직원 {employee_number} 데이터 분석 시작")
        
        if self.weekly_data is None:
            raise ValueError("데이터가 로드되지 않았습니다.")
            
        employee_data = self.weekly_data[
            self.weekly_data['employee_number'] == employee_number
        ].copy()
        
        if employee_data.empty:
            raise ValueError(f"직원번호 {employee_number}에 해당하는 데이터가 없습니다.")
        
        # 직원 정보 추출
        context = {
            "employee_info": self._extract_employee_info(employee_data),
            "team_goals": self._get_filtered_team_goals(employee_data),
            "team_criteria": self._get_filtered_team_criteria(employee_data),
            "weekly_tasks": employee_data[['start_date', 'end_date', 'done_task']].to_dict('records'),
            "reference_info": self._get_reference_info(employee_number)
        }
        
        logger.info(f"직원 {employee_number} 데이터 분석 완료")
        return context
    
    def _get_reference_info(self, employee_number: str) -> Dict[str, Any]:
        """참조 정보를 생성합니다."""
        # current_context에서 reference_data 가져오기 (load_and_preprocess_data에서 저장됨)
        reference_data = self.current_context.get("preprocessing_result", {}).get("reference_data", [])
        
        return {
            "source_type": "Pinecone Vector Database",
            "index_name": self.pinecone_index_name,
            "search_method": "metadata_filter",
            "filter_criteria": {"user_id": str(employee_number)},  # 문자열로 변환
            "documents_found": len(reference_data),
            "pinecone_matches": reference_data[:10],  # 상위 10개만 저장
            "data_sources": {
                "weekly_data": "Pinecone Vector DB",
                "team_criteria": "MariaDB.team_criteria",
                "team_goals": "MariaDB.team_goal"
            }
        }
    
    def _extract_employee_info(self, employee_data: pd.DataFrame) -> Dict[str, Any]:
        """직원 기본 정보를 추출합니다."""
        info = {
            "name": employee_data['name'].iloc[0] if 'name' in employee_data.columns else f"User_{employee_data['employee_number'].iloc[0]}",
            "employee_number": employee_data['employee_number'].iloc[0],
            "organization_id": employee_data['organization_id'].iloc[0] if 'organization_id' in employee_data.columns else "",
            "evaluation_year": employee_data['evaluation_year'].iloc[0] if 'evaluation_year' in employee_data.columns else "",
            "evaluation_quarter": employee_data['evaluation_quarter'].iloc[0] if 'evaluation_quarter' in employee_data.columns else "",
            "period": "",
            "total_weeks": len(employee_data),
            "total_activities": len(employee_data)
        }
        
        # 날짜 범위 설정
        if 'start_date' in employee_data.columns and 'end_date' in employee_data.columns:
            start_dates = pd.to_datetime(employee_data['start_date'], errors='coerce').dropna()
            end_dates = pd.to_datetime(employee_data['end_date'], errors='coerce').dropna()
            if not start_dates.empty and not end_dates.empty:
                info["period"] = f"{start_dates.min().strftime('%Y-%m-%d')} ~ {end_dates.max().strftime('%Y-%m-%d')}"
        
        logger.info(f"직원 정보: {info['name']} (조직ID: {info['organization_id']})")
        return info
    
    def _get_filtered_team_goals(self, employee_data: pd.DataFrame) -> List[Dict]:
        """해당 직원의 팀 목표만 필터링하여 반환합니다."""
        if self.team_goals is None or self.team_goals.empty:
            return []
            
        # organization_id로 필터링
        employee_org_id = employee_data['organization_id'].iloc[0] if 'organization_id' in employee_data.columns else ""
        
        if employee_org_id:
            # organization_id 컬럼 찾기
            org_column = self._find_column_by_keywords(
                self.team_goals, 
                ['organization_id', 'org_id', 'team_id', '조직', '팀']
            )
            
            if org_column:
                filtered_goals = self.team_goals[
                    self.team_goals[org_column].astype(str) == str(employee_org_id)
                ].to_dict('records')
                logger.info(f"팀 목표 필터링 완료: {len(filtered_goals)}개 목표")
                return filtered_goals
        
        logger.warning("조직 ID 매칭 실패, 전체 목표 반환")
        return self.team_goals.to_dict('records')
    
    def _get_filtered_team_criteria(self, employee_data: pd.DataFrame) -> List[Dict]:
        """해당 직원의 팀 평가 기준만 필터링하여 반환합니다."""
        if self.team_criteria is None or self.team_criteria.empty:
            return []
            
        # organization_id로 필터링
        employee_org_id = employee_data['organization_id'].iloc[0] if 'organization_id' in employee_data.columns else ""
        
        if employee_org_id:
            # organization_id 컬럼 찾기
            org_column = self._find_column_by_keywords(
                self.team_criteria, 
                ['organization_id', 'org_id', 'team_id', '조직', '팀']
            )
            
            if org_column:
                filtered_criteria = self.team_criteria[
                    self.team_criteria[org_column].astype(str) == str(employee_org_id)
                ].to_dict('records')
                logger.info(f"팀 기준 필터링 완료: {len(filtered_criteria)}개 기준")
                return filtered_criteria
        
        logger.warning("조직 ID 매칭 실패, 전체 기준 반환")
        return self.team_criteria.to_dict('records')
    
    def _convert_date_to_week_format(self, start_date: str, end_date: str) -> str:
        """날짜를 'N월 N주차' 형식으로 변환합니다."""
        try:
            from datetime import datetime
            import calendar
            
            # 시작 날짜 파싱
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            
            # 월과 해당 월의 몇 번째 주인지 계산
            month = start_dt.month
            
            # 해당 월의 첫 번째 날
            first_day = datetime(start_dt.year, month, 1)
            
            # 첫 번째 날의 요일 (0=월요일, 6=일요일)
            first_weekday = first_day.weekday()
            
            # 시작 날짜가 몇 번째 주인지 계산
            # 월의 첫 번째 월요일을 기준으로 주차 계산
            days_from_first = (start_dt - first_day).days
            week_number = (days_from_first + first_weekday) // 7 + 1
            
            return f"{month}월 {week_number}주차 weekly 보고서"
        
        except Exception as e:
            logger.warning(f"날짜 변환 실패 ({start_date} ~ {end_date}): {e}")
            return f"weekly 보고서 ({start_date}~{end_date})"
    
    def _find_column_by_keywords(self, 
                                dataframe: pd.DataFrame, 
                                keywords: List[str]) -> Optional[str]:
        """키워드를 기반으로 컬럼을 찾습니다."""
        for col in dataframe.columns:
            col_lower = str(col).lower().strip()
            if any(keyword.lower() in col_lower for keyword in keywords):
                return col
        return None
    
    def extract_team_goal_categories(self, team_goals: List[Dict]) -> List[str]:
        """팀 목표에서 카테고리를 추출합니다."""
        if not team_goals:
            logger.warning("팀 목표 데이터가 없습니다.")
            return []
        
        # 목표 관련 키 찾기
        goal_keywords = ['goal_name', 'task_name', 'objective', 'goal', 'task', '성과지표명', '과제명', '목표명']
        exclude_keywords = ['name', '이름', '성명']
        
        first_record = team_goals[0]
        goal_key = None
        
        # 정확한 매칭 우선
        for key in goal_keywords:
            if key in first_record:
                goal_key = key
                break
        
        # 키워드 포함 검색
        if not goal_key:
            for key in first_record.keys():
                key_lower = str(key).lower()
                if any(keyword in key_lower for keyword in ['goal', 'task', '과제', '목표', '지표']):
                    if not any(exclude in key_lower for exclude in exclude_keywords) or 'goal' in key_lower:
                        goal_key = key
                        break
        
        if goal_key:
            categories = list(set([
                str(record[goal_key]).strip() for record in team_goals 
                if record.get(goal_key) and str(record[goal_key]).strip() and str(record[goal_key]).strip().lower() != 'nan'
            ]))
            logger.info(f"카테고리 추출 완료: {categories}")
            return categories
        else:
            logger.error("목표 카테고리를 추출할 수 없습니다.")
            return []
    
    def generate_evaluation_prompt(self, employee_data: Dict[str, Any]) -> str:
        """평가용 프롬프트를 생성합니다."""
        
        team_categories = self.extract_team_goal_categories(employee_data['team_goals'])
        
        if not team_categories:
            # 기본 카테고리 사용
            team_categories = [
                "oud Professional 업무 진행 통한 BR/UR 개선",
                "CSP 파트너쉽 강화 통한 원가개선", 
                "oud 마케팅 및 홍보 통한 대외 oud 고객확보",
                "글로벌 사업 Tech-presales 진행"
            ]
            logger.warning(f"팀 목표 카테고리를 추출할 수 없어 기본 카테고리 사용: {team_categories}")
        
        prompt = f"""
당신은 전문적인 HR 평가 에이전트입니다. 직원의 주간 보고서를 종합 분석하여 객관적인 성과 평가를 수행해주세요.

## 평가 대상 정보

### 직원 기본 정보
- 이름: {employee_data['employee_info']['name']}
- 직원번호(User ID): {employee_data['employee_info']['employee_number']}
- 조직 ID: {employee_data['employee_info']['organization_id']}
- 평가 기간: {employee_data['employee_info']['period']}
- 평가 년도/분기: {employee_data['employee_info']['evaluation_year']}년 {employee_data['employee_info']['evaluation_quarter']}분기
- 총 평가 주차: {employee_data['employee_info']['total_weeks']}주

### 주간별 수행 업무
"""
        
        # 주간별 업무 추가
        if employee_data['weekly_tasks']:
            for i, task in enumerate(employee_data['weekly_tasks'], 1):
                start_date = task.get('start_date', 'N/A')
                end_date = task.get('end_date', 'N/A')
                done_task = task.get('done_task', 'N/A')
                prompt += f"\n**{i}주차 ({start_date} ~ {end_date})**\n"
                prompt += f"{done_task}\n"
        else:
            prompt += "\n주간 업무 데이터가 없습니다.\n"
        
        # 팀 목표 추가
        if employee_data['team_goals']:
            prompt += "\n### 팀 목표 및 성과지표\n"
            for i, goal in enumerate(employee_data['team_goals'], 1):
                prompt += f"**목표 {i}**: {goal}\n"
        
        # 팀 평가 기준 추가
        if employee_data['team_criteria']:
            prompt += "\n### 팀 평가 기준\n"
            for i, criteria in enumerate(employee_data['team_criteria'], 1):
                prompt += f"**기준 {i}**: {criteria}\n"
        
        prompt += f"""

## 평가 결과 형식

다음 JSON 형식으로 종합 평가를 제공해주세요:

```json
{{
  "teamGoals": [
    {{
      "goalName": "목표명",
      "assigned": "배정|미배정",
      "contributionCount": 기여활동수,
      "contents": [
        {{
          "description": "구체적인 업무 활동 설명",
          "reference": [
            {{
              "label": "N월 N주차 weekly 보고서",
              "excerpt": "해당 주차 보고서에서 관련 업무 내용 발췌"
            }}
          ]
        }}
      ]
    }}
  ]
}}
```

## 평가 가이드라인

1. **팀 목표 분류**: 주간 보고서의 실제 업무를 아래 목표들로 분류하세요:
"""
        
        for i, category in enumerate(team_categories, 1):
            prompt += f"   {i}. {category}\n"
        
        prompt += f"""

2. **배정 상태 판단**: 
   - "배정": 해당 목표와 관련된 구체적 활동이 1개 이상 있는 경우
   - "미배정": 해당 목표와 관련된 활동이 없거나 매우 간접적인 경우

3. **기여 활동 집계**:
   - contributionCount: contents 배열의 실제 길이와 일치해야 함
   - 실제 수행한 구체적 업무만 포함

4. **업무 내용 추출**:
   - description: 실제 수행한 구체적인 업무 활동
   - excerpt: 해당 주차 보고서에서 그 활동을 언급한 원문 발췌

5. **참조 정보**:
   - label: "N월 N주차 weekly 보고서" 형식
   - excerpt: 실제 주간 보고서에서 해당 활동을 언급한 문장 그대로 발췌

## 중요사항
- 모든 팀 목표를 배열에 포함해야 함 (활동이 없어도 "미배정"으로 포함)
- excerpt는 실제 주간 보고서의 원문을 그대로 발췌
- description은 여러 주차의 관련 활동을 종합하여 하나의 구체적 업무로 정리
- 날짜 변환: 2024-01-01~2024-01-07 → "1월 1주차 weekly 보고서"

JSON 형식을 정확히 준수하여 응답해주세요.
"""
        
        return prompt
    
    def execute_llm_evaluation(self, prompt: str) -> Dict[str, Any]:
        """LLM을 사용하여 평가를 실행합니다."""
        try:
            logger.info(f"LLM 평가 실행 - 모델: {self.model}")
            print(f"🤖 OpenAI API 호출 시작... (모델: {self.model})")
            
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system", 
                        "content": "당신은 전문적인 HR 평가 에이전트입니다. 객관적이고 구체적인 성과 평가를 제공하며, 항상 정확한 JSON 형식으로 응답합니다."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=4000
            )
            
            response_text = response.choices[0].message.content
            print(f"✅ OpenAI API 응답 수신 완료 (길이: {len(response_text)} 문자)")
            
            # JSON 추출 및 파싱
            json_text = self._extract_json_from_response(response_text)
            result = json.loads(json_text)
            
            logger.info("LLM 평가 완료")
            print("✅ JSON 파싱 성공")
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 오류: {e}")
            print(f"❌ JSON 파싱 실패: {e}")
            return {
                "error": "JSON 파싱 실패",
                "raw_response": response_text if 'response_text' in locals() else "No response",
                "error_details": str(e)
            }
        except Exception as e:
            logger.error(f"LLM 호출 오류: {e}")
            print(f"❌ OpenAI API 호출 실패: {e}")
            return {"error": str(e)}
    
    def _extract_json_from_response(self, response_text: str) -> str:
        """응답에서 JSON 부분을 추출합니다."""
        if "```json" in response_text:
            json_start = response_text.find("```json") + 7
            json_end = response_text.find("```", json_start)
            return response_text[json_start:json_end].strip()
        else:
            return response_text.strip()
    
    def save_evaluation_results(self, 
                               results: Dict[str, Any], 
                               reference_info: Dict[str, Any],
                               filename: Optional[str] = None) -> str:
        """평가 결과를 새로운 teamGoals JSON 형식으로 저장합니다."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"evaluation_result_{timestamp}.json"
        
        # 새로운 JSON 형식으로 변환
        if "error" not in results and "teamGoals" in results:
            # 이미 새로운 형식인 경우 그대로 사용
            final_results = results
        elif "error" not in results:
            # 기존 형식에서 새로운 형식으로 변환 (호환성을 위해)
            team_categories = [
                "Cloud Professional 업무 진행 통한 BR/UR 개선",
                "CSP 파트너쉽 강화 통한 원가개선", 
                "Cloud 마케팅 및 홍보 통한 대외 Cloud 고객확보",
                "글로벌 사업 Tech-presales 진행"
            ]
            
            final_results = {
                "teamGoals": []
            }
            
            # 각 팀 목표에 대해 기본 구조 생성
            for goal_name in team_categories:
                goal_data = {
                    "goalName": goal_name,
                    "assigned": "미배정",
                    "contributionCount": 0,
                    "contents": []
                }
                final_results["teamGoals"].append(goal_data)
                
        else:
            # 오류가 있는 경우 기존 형식 유지
            final_results = results
        
        # reference 정보 추가
        final_results["reference"] = {
            "evaluation_basis": "이 평가는 Pinecone 벡터 데이터베이스의 주간 보고서 데이터와 MariaDB의 팀 목표/기준 데이터를 기반으로 AI가 분석하여 생성되었습니다.",
            "data_sources": reference_info.get("data_sources", {}),
            "pinecone_info": {
                "index_name": reference_info.get("index_name", ""),
                "search_method": reference_info.get("search_method", ""),
                "filter_criteria": reference_info.get("filter_criteria", {}),
                "documents_found": reference_info.get("documents_found", 0),
                "namespace": getattr(self, 'namespace', '')
            },
            "evaluation_timestamp": datetime.now().isoformat(),
            "system_info": {
                "ai_model": self.model,
                "pinecone_index": self.pinecone_index_name,
                "database": f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            }
        }
        
        output_file = self.output_path / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"평가 결과 저장 완료: {output_file}")
        return str(output_file)
    
    def _is_activity_related(self, activity: str, done_task: str) -> bool:
        """활동과 업무 내용의 연관성을 판단합니다."""
        try:
            # 간단한 키워드 매칭
            activity_keywords = activity.lower().split()
            done_task_lower = done_task.lower()
            
            # 주요 키워드가 포함되어 있는지 확인
            match_count = sum(1 for keyword in activity_keywords if keyword in done_task_lower and len(keyword) > 2)
            
            # 전체 키워드의 30% 이상 매칭되면 관련성 있다고 판단
            return match_count >= max(1, len(activity_keywords) * 0.3)
        except:
            return False
    
    def execute_single_evaluation(self, 
                                 user_id: str) -> Dict[str, Any]:
        """단일 사용자에 대한 완전한 평가를 실행합니다."""
        
        logger.info(f"=== 사용자 {user_id} 평가 시작 ===")
        
        try:
            # 1단계: 계획 수립
            plan = self.plan_evaluation(user_id)
            
            # 2단계: 데이터 소스 검증
            validation = self.validate_data_sources(user_id)
            if not validation["valid"]:
                raise ValueError(f"데이터 소스 검증 실패: {validation['errors']}")
            
            # 3단계: 데이터 로드
            preprocessing_result = self.load_and_preprocess_data(user_id)
            
            # 전처리 결과를 컨텍스트에 저장 (reference 정보를 위해)
            self.current_context["preprocessing_result"] = preprocessing_result
            
            # 4단계: 직원 데이터 분석
            employee_data = self.analyze_employee_data(user_id)
            
            # 5단계: 프롬프트 생성
            prompt = self.generate_evaluation_prompt(employee_data)
            
            # 6단계: LLM 평가 실행
            evaluation_result = self.execute_llm_evaluation(prompt)
            
            # 7단계: 결과 저장 (reference 정보 포함)
            if "error" not in evaluation_result:
                output_file = self.save_evaluation_results(
                    evaluation_result,
                    employee_data["reference_info"],  # reference 정보 전달
                    f"evaluation_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                
                # 평가 이력에 추가
                self.evaluation_history.append({
                    "user_id": user_id,
                    "timestamp": datetime.now().isoformat(),
                    "output_file": output_file,
                    "status": "success"
                })
                
                logger.info(f"사용자 {user_id} 평가 완료 - 성공")
                return evaluation_result
            else:
                # 오류 발생 시에도 간단한 결과 저장
                error_result = {
                    "user_id": user_id,
                    "timestamp": datetime.now().isoformat(),
                    "error": evaluation_result["error"]
                }
                
                output_file = self.save_evaluation_results(
                    error_result,
                    {"error": "평가 실패로 인한 제한된 참조 정보"},
                    f"evaluation_error_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                
                self.evaluation_history.append({
                    "user_id": user_id,
                    "timestamp": datetime.now().isoformat(),
                    "output_file": output_file,
                    "status": "failed"
                })
                
                logger.info(f"사용자 {user_id} 평가 실패")
                return error_result
                
        except Exception as e:
            logger.error(f"사용자 {user_id} 평가 실패: {str(e)}")
            error_result = {
                "user_id": user_id,
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
            return error_result

    def execute_batch_evaluation(self, 
                                target_user_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """다수 사용자에 대한 배치 평가를 실행합니다."""
        
        logger.info("=== 배치 평가 시작 ===")
        
        try:
            # 대상 사용자 목록 결정 (Pinecone에서 모든 user_id 조회)
            if target_user_ids is None:
                # get_available_user_ids 메서드를 사용하여 네임스페이스까지 고려한 조회
                target_user_ids = self.get_available_user_ids()
                logger.info(f"Pinecone에서 발견된 사용자 ID: {target_user_ids}")
            
            if not target_user_ids:
                logger.warning("사용 가능한 사용자 ID가 없습니다.")
                return {
                    "batch_metadata": {
                        "start_time": datetime.now().isoformat(),
                        "error": "사용 가능한 사용자 ID가 없습니다.",
                        "namespace": self.namespace
                    },
                    "batch_summary": {
                        "successful_evaluations": 0,
                        "failed_evaluations": 0
                    }
                }
            
            batch_results = {
                "batch_metadata": {
                    "start_time": datetime.now().isoformat(),
                    "target_user_ids": target_user_ids,
                    "total_users": len(target_user_ids),
                    "namespace": self.namespace,
                    "data_sources": {
                        "weekly_data": f"Pinecone Index: {self.pinecone_index_name}",
                        "team_data": f"MariaDB: {self.db_config['host']}/{self.db_config['database']}"
                    }
                },
                "individual_results": {},
                "batch_summary": {
                    "successful_evaluations": 0,
                    "failed_evaluations": 0
                }
            }
            
            # 개별 사용자 평가 실행
            for user_id in target_user_ids:
                logger.info(f"배치 평가 진행 중: {user_id}")
                
                try:
                    result = self.execute_single_evaluation(user_id)
                    
                    batch_results["individual_results"][user_id] = result
                    
                    if "error" not in result:
                        batch_results["batch_summary"]["successful_evaluations"] += 1
                    else:
                        batch_results["batch_summary"]["failed_evaluations"] += 1
                        
                except Exception as e:
                    logger.error(f"사용자 {user_id} 배치 평가 실패: {str(e)}")
                    batch_results["individual_results"][user_id] = {
                        "error": str(e),
                        "user_id": user_id
                    }
                    batch_results["batch_summary"]["failed_evaluations"] += 1
            
            batch_results["batch_metadata"]["end_time"] = datetime.now().isoformat()
            
            # 배치 결과 저장 (reference 정보 포함)
            batch_reference_info = {
                "data_sources": {
                    "weekly_data": "Pinecone Vector DB",
                    "team_criteria": "MariaDB.team_criteria", 
                    "team_goals": "MariaDB.team_goal"
                },
                "pinecone_matches": [],  # 배치에서는 개별 매치 정보 제외
                "documents_found": 0,
                "namespace": self.namespace
            }
            
            batch_output_file = self.save_evaluation_results(
                batch_results,
                batch_reference_info,
                f"batch_evaluation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            
            logger.info(f"배치 평가 완료 - 성공: {batch_results['batch_summary']['successful_evaluations']}명, "
                       f"실패: {batch_results['batch_summary']['failed_evaluations']}명")
            
            return batch_results
            
        except Exception as e:
            logger.error(f"배치 평가 실패: {str(e)}")
            return {
                "batch_metadata": {
                    "start_time": datetime.now().isoformat(),
                    "error": str(e),
                    "namespace": self.namespace
                },
                "batch_summary": {
                    "successful_evaluations": 0,
                    "failed_evaluations": len(target_user_ids) if target_user_ids else 0
                }
            }

    def get_evaluation_history(self) -> List[Dict[str, Any]]:
        """평가 이력을 반환합니다."""
        return self.evaluation_history

    def get_available_user_ids(self) -> List[str]:
        """Pinecone에서 사용 가능한 모든 user_id를 조회합니다."""
        logger.info("Pinecone에서 사용 가능한 user_id 조회 시작")
        
        try:
            # 먼저 인덱스 상태 확인
            stats = self.index.describe_index_stats()
            total_vectors = stats.total_vector_count
            print(f"🔍 인덱스 통계: 총 {total_vectors}개 벡터")
            
            if total_vectors == 0:
                logger.warning("인덱스에 벡터가 없습니다.")
                return []
            
            # 여러 방법으로 데이터 조회 시도
            user_ids = set()
            
            # 방법 1: 기본 쿼리 (더미 벡터 사용)
            dummy_vector = [0.0] * 1024
            
            # 더 많은 결과를 얻기 위해 여러 번 쿼리
            for attempt in range(3):
                try:
                    # 매번 다른 더미 벡터 사용
                    if attempt > 0:
                        dummy_vector = [random.uniform(-1, 1) for _ in range(1024)]
                    
                    query_result = self.index.query(
                        vector=dummy_vector,
                        top_k=min(10000, total_vectors),  # 가능한 최대값
                        include_metadata=True
                    )
                    
                    print(f"🔄 시도 {attempt + 1}: {len(query_result.matches)}개 결과")
                    
                    for match in query_result.matches:
                        if hasattr(match, 'metadata') and match.metadata:
                            # 다양한 키 이름 확인
                            for key in ['user_id', 'userId', 'USER_ID', 'employee_id', 'emp_id']:
                                if key in match.metadata:
                                    user_ids.add(str(match.metadata[key]))
                                    
                            # 메타데이터 키 디버깅 (첫 번째 매치에서만)
                            if attempt == 0 and len(user_ids) == 0:
                                print(f"🔍 메타데이터 키 확인: {list(match.metadata.keys())}")
                                print(f"🔍 샘플 메타데이터: {dict(list(match.metadata.items())[:5])}")
                    
                    if user_ids:
                        break
                        
                except Exception as e:
                    print(f"❌ 시도 {attempt + 1} 실패: {e}")
                    continue
            
            # 방법 2: 네임스페이스 확인 및 검색
            if not user_ids:
                print("🔄 네임스페이스별 검색 시도...")
                try:
                    if hasattr(stats, 'namespaces') and stats.namespaces:
                        for namespace in stats.namespaces.keys():
                            print(f"🔍 네임스페이스 '{namespace}' 검색 중...")
                            namespace_result = self.index.query(
                                vector=dummy_vector,
                                top_k=1000,
                                include_metadata=True,
                                namespace=namespace
                            )
                            
                            for match in namespace_result.matches:
                                if hasattr(match, 'metadata') and match.metadata:
                                    for key in ['user_id', 'userId', 'USER_ID', 'employee_id', 'emp_id']:
                                        if key in match.metadata:
                                            user_ids.add(str(match.metadata[key]))
                            
                            if user_ids:
                                print(f"✅ 네임스페이스 '{namespace}'에서 {len(user_ids)}개 user_id 발견")
                                break
                except Exception as e:
                    print(f"❌ 네임스페이스 검색 실패: {e}")
            
            # 방법 3: fetch API 사용 (ID를 알고 있는 경우)
            if not user_ids:
                print("🔄 Fetch API 시도...")
                try:
                    # 인덱스에서 일부 ID 가져오기
                    first_query = self.index.query(
                        vector=dummy_vector,
                        top_k=10,
                        include_metadata=False  # ID만 가져오기
                    )
                    
                    if first_query.matches:
                        # 첫 번째 ID들로 fetch 시도
                        ids_to_fetch = [match.id for match in first_query.matches[:5]]
                        fetch_result = self.index.fetch(ids=ids_to_fetch)
                        
                        for id_key, vector_data in fetch_result.vectors.items():
                            if hasattr(vector_data, 'metadata') and vector_data.metadata:
                                for key in ['user_id', 'userId', 'USER_ID', 'employee_id', 'emp_id']:
                                    if key in vector_data.metadata:
                                        user_ids.add(str(vector_data.metadata[key]))
                                        
                                if not user_ids:  # 첫 번째에서 메타데이터 키 확인
                                    print(f"🔍 Fetch 메타데이터 키: {list(vector_data.metadata.keys())}")
                        
                except Exception as e:
                    print(f"❌ Fetch API 실패: {e}")
            
            available_ids = sorted(list(user_ids))
            
            if available_ids:
                logger.info(f"사용 가능한 user_id: {available_ids}")
                print(f"✅ 총 {len(available_ids)}명의 사용자 발견: {available_ids}")
            else:
                logger.warning("user_id를 찾을 수 없습니다. 메타데이터 구조를 확인해주세요.")
                print("❌ user_id를 찾을 수 없습니다.")
                
                # 디버깅을 위한 추가 정보 출력
                try:
                    debug_query = self.index.query(
                        vector=dummy_vector,
                        top_k=1,
                        include_metadata=True
                    )
                    if debug_query.matches:
                        print(f"🔍 디버깅: 전체 메타데이터 구조")
                        print(f"    {debug_query.matches[0].metadata}")
                except:
                    pass
            
            return available_ids
            
        except Exception as e:
            logger.error(f"user_id 조회 실패: {str(e)}")
            print(f"❌ 전체 조회 실패: {e}")
            return []

    def debug_pinecone_search(self, user_id: str) -> Dict[str, Any]:
        """Pinecone 검색 디버깅을 위한 함수"""
        logger.info(f"Pinecone 검색 디버깅 - user_id: {user_id}")
        
        try:
            dummy_vector = [0.0] * 1024
            
            # 1. 전체 검색 (필터 없음)
            all_results = self.index.query(
                vector=dummy_vector,
                top_k=10,
                include_metadata=True
            )
            
            print(f"🔍 전체 검색 결과 (상위 10개):")
            for i, match in enumerate(all_results.matches, 1):
                metadata = match.metadata
                print(f"  {i}. ID: {match.id}")
                print(f"     user_id: {metadata.get('user_id')} (타입: {type(metadata.get('user_id'))})")
                print(f"     organization_id: {metadata.get('organization_id')}")
                print(f"     start_date: {metadata.get('start_date')}")
                print()
            
            # 2. 특정 user_id로 필터링
            filtered_results = self.index.query(
                vector=dummy_vector,
                filter={"user_id": str(user_id)},
                top_k=10,
                include_metadata=True
            )
            
            print(f"🎯 user_id '{user_id}' 필터링 결과:")
            print(f"   찾은 결과 수: {len(filtered_results.matches)}")
            
            for i, match in enumerate(filtered_results.matches, 1):
                metadata = match.metadata
                print(f"  {i}. ID: {match.id}")
                print(f"     user_id: {metadata.get('user_id')}")
                print(f"     done_task: {metadata.get('done_task', '')[:100]}...")
                print()
            
            return {
                "total_results": len(all_results.matches),
                "filtered_results": len(filtered_results.matches),
                "available_user_ids": [str(match.metadata.get('user_id')) for match in all_results.matches if 'user_id' in match.metadata]
            }
            
        except Exception as e:
            logger.error(f"디버깅 검색 실패: {str(e)}")
            return {"error": str(e)}

    def get_evaluation_statistics(self) -> Dict[str, Any]:
        """평가 통계를 반환합니다."""
        if not self.evaluation_history:
            return {"total_evaluations": 0}
        
        successful_evaluations = [
            entry for entry in self.evaluation_history 
            if entry.get("status") == "success"
        ]
        
        stats = {
            "total_evaluations": len(self.evaluation_history),
            "successful_evaluations": len(successful_evaluations),
            "failed_evaluations": len(self.evaluation_history) - len(successful_evaluations),
            "latest_evaluation": self.evaluation_history[-1]["timestamp"] if self.evaluation_history else None,
            "evaluated_users": [entry["user_id"] for entry in self.evaluation_history]
        }
        
        return stats


def main():
    """메인 실행 함수"""
    
    print("🎯 === 최종 주간 보고서 평가 시스템 ===")
    print("📋 Pinecone + MariaDB 기반 AI 평가 시스템")
    
    # 환경변수에서 API 키 가져오기
    openai_key = os.getenv("OPENAI_API_KEY")
    pinecone_key = os.getenv("PINECONE_API_KEY")
    model = os.getenv("OPENAI_MODEL", "gpt-4-turbo")  # 기본값 설정
    output_path = os.getenv("OUTPUT_PATH", "./output")  # 기본값 설정
    
    # API 키 검증
    if not openai_key:
        print("❌ OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
        print("💡 .env 파일을 확인하거나 환경변수를 설정해주세요.")
        return
    
    if not pinecone_key:
        print("❌ PINECONE_API_KEY 환경변수가 설정되지 않았습니다.")
        print("💡 .env 파일을 확인하거나 환경변수를 설정해주세요.")
        return
    
    try:
        print(f"\n🤖 시스템 초기화 중... (모델: {model})")
        
        # 에이전트 초기화
        agent = WeeklyReportEvaluationAgent(
            openai_api_key=openai_key,
            pinecone_api_key=pinecone_key,
            model=model,
            output_path=output_path
        )
        
        print("✅ 시스템 초기화 완료!")
        
        # 나머지 메뉴 코드는 동일...
        # 사용자 메뉴
        while True:
            print(f"\n🎯 === 메인 메뉴 ===")
            print("1. 모든 사용자 목록 조회")
            print("2. 단일 사용자 평가")
            print("3. 배치 평가 (모든 사용자)")
            print("4. 평가 통계 확인")
            print("5. 종료")
            
            choice = input("\n선택하세요 (1-5): ").strip()
            
            if choice == "1":
                print("\n📋 모든 사용자 목록 조회 중...")
                available_users = agent.get_available_user_ids()
                print(f"\n📊 총 {len(available_users)}명의 사용자:")
                for i, user_id in enumerate(available_users, 1):
                    print(f"  {i:2d}. User {user_id}")
            
            elif choice == "2":
                available_users = agent.get_available_user_ids()
                if not available_users:
                    print("❌ 사용 가능한 사용자가 없습니다.")
                    continue
                
                print(f"\n사용 가능한 사용자: {available_users}")
                user_id = input("평가할 사용자 ID를 입력하세요: ").strip()
                
                if user_id not in available_users:
                    print(f"❌ 사용자 ID '{user_id}'는 존재하지 않습니다.")
                    continue
                
                print(f"\n🚀 사용자 {user_id} 평가 시작...")
                result = agent.execute_single_evaluation(user_id)
                
                # 결과 요약 출력
                if "error" not in result:
                    emp_info = result.get('employee_summary', {}).get('basic_info', {})
                    print(f"\n🎉 === 평가 완료 ===")
                    print(f"✅ 사용자: {emp_info.get('name', user_id)}")
                    print(f"📊 총 활동: {emp_info.get('total_activities', 0)}건")
                    print(f"📅 평가 기간: {emp_info.get('period', 'N/A')}")
                    
                    activities = result.get('employee_summary', {}).get('activity_categorization', [])
                    print(f"\n📋 카테고리별 활동:")
                    for activity in activities:
                        print(f"   - {activity.get('category', 'Unknown')}: {activity.get('count', 0)}건")
                else:
                    print(f"\n❌ 평가 실패: {result['error']}")
            
            elif choice == "3":
                available_users = agent.get_available_user_ids()
                if not available_users:
                    print("❌ 사용 가능한 사용자가 없습니다.")
                    continue
                
                print(f"\n📊 총 {len(available_users)}명의 사용자 배치 평가를 시작합니다.")
                confirm = input("계속하시겠습니까? (y/N): ").strip().lower()
                
                if confirm not in ['y', 'yes']:
                    print("❌ 배치 평가를 취소합니다.")
                    continue
                
                print(f"\n🚀 배치 평가 시작...")
                batch_result = agent.execute_batch_evaluation()
                
                # 배치 결과 요약
                print(f"\n🎉 === 배치 평가 결과 ===")
                print(f"📊 총 대상: {batch_result['batch_metadata']['total_users']}명")
                print(f"✅ 성공: {batch_result['batch_summary']['successful_evaluations']}건")
                print(f"❌ 실패: {batch_result['batch_summary']['failed_evaluations']}건")
                print(f"📈 성공률: {(batch_result['batch_summary']['successful_evaluations']/batch_result['batch_metadata']['total_users']*100):.1f}%")
            
            elif choice == "4":
                stats = agent.get_evaluation_statistics()
                print(f"\n📈 === 평가 통계 ===")
                print(f"총 평가 수행: {stats['total_evaluations']}건")
                print(f"성공한 평가: {stats['successful_evaluations']}건")
                print(f"실패한 평가: {stats['failed_evaluations']}건")
                if stats['total_evaluations'] > 0:
                    print(f"성공률: {(stats['successful_evaluations']/stats['total_evaluations']*100):.1f}%")
                print(f"최근 평가: {stats['latest_evaluation']}")
                print(f"평가한 사용자: {stats['evaluated_users']}")
            
            elif choice == "5":
                print("👋 시스템을 종료합니다.")
                break
            
            else:
                print("❌ 잘못된 선택입니다. 1-5 중에서 선택해주세요.")
        
    except Exception as e:
        logger.error(f"메인 실행 오류: {str(e)}")
        print(f"❌ 시스템 오류: {e}")
        raise


if __name__ == "__main__":
    main()