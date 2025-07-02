import pandas as pd
import json
import openai
from typing import Dict, List, Any, Optional, Tuple
import os
from datetime import datetime
from pathlib import Path
import logging
from pymongo import MongoClient
import random
import pymysql
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBWeeklyReportAgent:
    def __init__(self, 
                 openai_api_key: Optional[str] = None,
                 model: Optional[str] = None,
                 output_path: Optional[str] = None):
        """
        MongoDB 기반 주간 보고서 평가 에이전트 - 환경변수 버전
        weekly_evaluation_results 컬렉션에서 데이터를 로드하여 AI 평가 수행
        
        Args:
            openai_api_key: OpenAI API 키 (None인 경우 환경변수에서 로드)
            model: 사용할 LLM 모델명 (None인 경우 환경변수에서 로드)
            output_path: 결과 파일들을 저장할 경로 (None인 경우 환경변수에서 로드)
        """
        # 환경변수에서 설정 로드
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4-turbo")
        self.output_path = Path(output_path or os.getenv("OUTPUT_PATH", "./output"))
        
        # 필수 환경변수 검증
        if not self.openai_api_key:
            raise ValueError(
                "OpenAI API 키가 설정되지 않았습니다. "
                "OPENAI_API_KEY 환경변수를 확인하세요."
            )
        
        self.openai_client = openai.OpenAI(api_key=self.openai_api_key)
        
        # MongoDB 연결 정보 - 환경변수에서 로드
        self.mongodb_config = {
            'host': os.getenv('MONGO_HOST', 'localhost'),
            'port': int(os.getenv('MONGO_PORT', 27017)),
            'database': os.getenv('MONGO_DB_NAME', 'skala'),
            'collection': 'weekly_evaluation_results',
            'username': os.getenv('MONGO_USER'),
            'password': os.getenv('MONGO_PASSWORD')
        }
        
        # MariaDB 연결 정보 - 환경변수에서 로드
        self.mariadb_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'database': os.getenv('DB_NAME', 'skala'),
            'username': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'charset': os.getenv('DB_CHARSET', 'utf8mb4')
        }
        
        # 필수 환경변수 검증
        self._validate_config()
        
        # MongoDB 클라이언트 초기화
        self.mongo_client = None
        self.db = None
        self.collection = None
        
        # MariaDB 연결 초기화
        self.mariadb_connection = None
        
        # 데이터 저장소
        self.evaluation_data = None
        
        # 에이전트 상태 추적
        self.evaluation_history = []
        self.current_context = {}
        
        # 출력 디렉토리 생성
        self.output_path.mkdir(exist_ok=True)
        
        logger.info(f"MongoDBWeeklyReportAgent 초기화 완료 - 모델: {self.model}")
    
    def _validate_config(self):
        """필수 환경변수 검증"""
        required_vars = {
            'OPENAI_API_KEY': self.openai_api_key,
            'MONGO_HOST': self.mongodb_config['host'],
            'MONGO_DB_NAME': self.mongodb_config['database'],
            'DB_HOST': self.mariadb_config['host'],
            'DB_USER': self.mariadb_config['username'],
            'DB_PASSWORD': self.mariadb_config['password'],
            'DB_NAME': self.mariadb_config['database']
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        
        if missing_vars:
            raise ValueError(
                f"필수 환경변수가 설정되지 않았습니다: {', '.join(missing_vars)}\n"
                f".env 파일을 확인하세요."
            )
        
        logger.info("✅ 모든 필수 환경변수가 정상적으로 로드되었습니다.")
    
    def connect_to_mariadb(self):
        """MariaDB에 연결합니다."""
        try:
            print(f"🔗 MariaDB 연결 시도: {self.mariadb_config['host']}:{self.mariadb_config['port']}")
            
            self.mariadb_connection = pymysql.connect(
                host=self.mariadb_config['host'],
                port=self.mariadb_config['port'],
                user=self.mariadb_config['username'],
                password=self.mariadb_config['password'],
                database=self.mariadb_config['database'],
                charset=self.mariadb_config['charset'],
                autocommit=True
            )
            
            print("✅ MariaDB 연결 성공!")
            logger.info(f"MariaDB 연결 성공: {self.mariadb_config['database']}")
            
            return True
            
        except Exception as e:
            logger.error(f"MariaDB 연결 실패: {str(e)}")
            print(f"❌ MariaDB 연결 실패: {e}")
            return False
    
    def get_user_name_from_mariadb(self, user_id: str) -> str:
        """MariaDB users 테이블에서 실제 사용자 이름을 조회합니다."""
        try:
            if not self.mariadb_connection:
                if not self.connect_to_mariadb():
                    return f"User_{user_id}"
            
            with self.mariadb_connection.cursor() as cursor:
                # users 테이블에서 id가 user_id와 일치하는 name을 조회
                sql = "SELECT name FROM users WHERE id = %s"
                cursor.execute(sql, (user_id,))
                result = cursor.fetchone()
                
                if result:
                    user_name = result[0]
                    print(f"📋 MariaDB에서 사용자 이름 조회 성공: {user_id} -> {user_name}")
                    logger.info(f"사용자 {user_id}의 실제 이름: {user_name}")
                    return user_name
                else:
                    print(f"⚠️ MariaDB에서 사용자 {user_id}를 찾을 수 없습니다.")
                    return f"User_{user_id}"
                    
        except Exception as e:
            logger.error(f"MariaDB 사용자 이름 조회 실패: {str(e)}")
            print(f"❌ MariaDB 사용자 이름 조회 실패: {e}")
            return f"User_{user_id}"
    
    def connect_to_mongodb(self):
        """MongoDB에 연결합니다."""
        try:
            # 인증 정보가 있는 경우와 없는 경우 처리
            if self.mongodb_config.get('username') and self.mongodb_config.get('password'):
                connection_string = f"mongodb://{self.mongodb_config['username']}:{self.mongodb_config['password']}@{self.mongodb_config['host']}:{self.mongodb_config['port']}/{self.mongodb_config['database']}?authSource=admin"
            else:
                connection_string = f"mongodb://{self.mongodb_config['host']}:{self.mongodb_config['port']}/"
            
            print(f"🔗 MongoDB 연결 시도: {self.mongodb_config['host']}:{self.mongodb_config['port']}")
            
            # MongoDB 클라이언트 생성
            self.mongo_client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000
            )
            
            # 연결 테스트
            print("🔄 연결 테스트 중...")
            self.mongo_client.admin.command('ping')
            print("✅ Ping 성공!")
            
            # 데이터베이스 및 컬렉션 설정
            self.db = self.mongo_client[self.mongodb_config['database']]
            self.collection = self.db[self.mongodb_config['collection']]
            
            # 사용 가능한 컬렉션 목록 확인
            collection_list = self.db.list_collection_names()
            print(f"📂 사용 가능한 컬렉션: {collection_list}")
            
            # 대상 컬렉션이 없으면 사용 가능한 컬렉션 중 하나 사용
            if self.mongodb_config['collection'] not in collection_list:
                if collection_list:
                    suggested_collection = collection_list[0]
                    print(f"⚠️ '{self.mongodb_config['collection']}' 컬렉션이 없습니다.")
                    print(f"💡 '{suggested_collection}' 컬렉션을 사용하시겠습니까?")
                    self.mongodb_config['collection'] = suggested_collection
                    self.collection = self.db[suggested_collection]
                else:
                    raise ValueError("사용 가능한 컬렉션이 없습니다.")
            
            # 문서 수 확인
            doc_count = self.collection.count_documents({})
            print(f"📊 '{self.mongodb_config['collection']}' 컬렉션 문서 수: {doc_count}개")
            
            logger.info(f"MongoDB 연결 성공: {self.mongodb_config['database']}.{self.mongodb_config['collection']}")
            logger.info(f"총 문서 수: {doc_count}개")
            
            return True
            
        except Exception as e:
            logger.error(f"MongoDB 연결 실패: {str(e)}")
            print(f"❌ 연결 실패: {e}")
            raise ValueError(f"MongoDB 연결 중 오류 발생: {str(e)}")
    
    def get_available_user_ids(self) -> List[str]:
        """MongoDB에서 사용 가능한 모든 user_id를 조회합니다."""
        logger.info("MongoDB에서 사용 가능한 user_id 조회 시작")
        
        try:
            if not self.mongo_client:
                self.connect_to_mongodb()
            
            print("🔍 딕셔너리 구조에서 user_id 추출 중...")
            
            # 첫 번째 문서에서 users 딕셔너리의 키들 가져오기
            first_doc = self.collection.find_one()
            
            if not first_doc or 'users' not in first_doc:
                print("❌ users 필드를 찾을 수 없습니다.")
                return []
            
            users_dict = first_doc['users']
            if not isinstance(users_dict, dict):
                print("❌ users가 딕셔너리가 아닙니다.")
                return []
            
            # 딕셔너리 키들 중에서 숫자 형태의 user_id만 필터링
            all_keys = list(users_dict.keys())
            user_ids = []
            
            for key in all_keys:
                # 'evaluation' 같은 키는 제외하고 숫자 형태만 포함
                if key.isdigit() or (isinstance(key, str) and key not in ['evaluation', 'metadata', 'summary']):
                    # 실제로 user_id 필드가 있는지 확인
                    user_data = users_dict[key]
                    if isinstance(user_data, dict) and 'user_id' in user_data:
                        user_ids.append(key)
            
            print(f"✅ 추출된 사용자 ID: {user_ids}")
            logger.info(f"사용 가능한 user_id: {user_ids}")
            return sorted(user_ids, key=lambda x: int(x) if x.isdigit() else float('inf'))
            
        except Exception as e:
            logger.error(f"user_id 조회 실패: {str(e)}")
            print(f"❌ 조회 실패: {e}")
            return []
    
    def load_user_data_from_mongodb(self, user_id: str) -> Dict[str, Any]:
        """MongoDB에서 특정 사용자의 모든 데이터를 로드합니다."""
        logger.info(f"MongoDB에서 사용자 {user_id} 데이터 로드 시작")
        
        try:
            if not self.mongo_client:
                self.connect_to_mongodb()
            
            print(f"🔍 딕셔너리 구조에서 사용자 {user_id} 데이터 검색 중...")
            
            # 첫 번째 문서에서 특정 사용자 데이터 추출
            first_doc = self.collection.find_one()
            
            if not first_doc or 'users' not in first_doc:
                raise ValueError("users 필드를 찾을 수 없습니다.")
            
            users_dict = first_doc['users']
            
            if user_id not in users_dict:
                raise ValueError(f"사용자 ID {user_id}에 해당하는 데이터가 MongoDB에 없습니다.")
            
            user_data = users_dict[user_id]
            
            logger.info(f"사용자 {user_id} 데이터 로드 완료")
            print(f"✅ 사용자 {user_id}: 데이터 발견")
            
            # 분기별 데이터를 개별 레코드로 변환
            user_records = []
            
            if 'quarters' in user_data and isinstance(user_data['quarters'], dict):
                for quarter_key, quarter_data in user_data['quarters'].items():
                    if isinstance(quarter_data, dict):
                        # 기본 사용자 정보 + 분기별 정보 결합
                        record = {
                            'user_id': user_data.get('user_id', user_id),
                            'name': user_data.get('name', f'User_{user_id}'),
                            'employee_number': user_data.get('employee_number', user_id),
                            'total_activities': user_data.get('total_activities', 0),
                            'quarter_key': quarter_key,
                            **quarter_data  # 분기별 데이터 추가
                        }
                        user_records.append(record)
            else:
                # 분기 데이터가 없으면 기본 사용자 데이터만
                user_records.append(user_data)
            
            if not user_records:
                raise ValueError(f"사용자 ID {user_id}에 유효한 데이터가 없습니다.")
            
            # 🔧 새로운 접근법: 복잡한 객체들을 안전하게 처리
            processed_documents = []
            for doc in user_records:
                processed_doc = {}
                
                # 모든 필드를 안전하게 처리
                for key, value in doc.items():
                    try:
                        if value is None:
                            processed_doc[key] = None
                        elif isinstance(value, (str, int, float, bool)):
                            # 기본 타입은 그대로 유지
                            processed_doc[key] = value
                        elif isinstance(value, list):
                            # 리스트는 별도 컬럼에 보존하고 문자열로 변환
                            processed_doc[f"{key}_list"] = str(value)  # JSON 문자열로 변환
                            processed_doc[f"{key}_count"] = len(value)  # 개수 정보
                            # 리스트 내용을 문자열로 결합
                            if value:
                                str_items = []
                                for item in value:
                                    if isinstance(item, dict):
                                        # 딕셔너리인 경우 주요 필드만 추출
                                        if 'description' in item:
                                            str_items.append(str(item['description']))
                                        elif 'goalName' in item:
                                            str_items.append(str(item['goalName']))
                                        else:
                                            str_items.append(str(item))
                                    else:
                                        str_items.append(str(item))
                                processed_doc[key] = '\n'.join(str_items)
                            else:
                                processed_doc[key] = ''
                        elif isinstance(value, dict):
                            # 딕셔너리는 JSON 문자열로 변환
                            processed_doc[f"{key}_dict"] = str(value)
                            # 딕셔너리의 주요 필드들 추출
                            if 'id' in value:
                                processed_doc[f"{key}_id"] = value['id']
                            if 'name' in value:
                                processed_doc[f"{key}_name"] = value['name']
                            if 'description' in value:
                                processed_doc[f"{key}_description"] = value['description']
                            # 딕셔너리 자체는 문자열로 변환
                            processed_doc[key] = str(value)
                        else:
                            # 기타 타입은 문자열로 변환
                            processed_doc[key] = str(value)
                            
                    except Exception as field_error:
                        # 개별 필드 처리 실패 시 로그 남기고 계속 진행
                        logger.warning(f"필드 {key} 처리 실패: {field_error}")
                        processed_doc[key] = f"처리실패: {str(value)[:100]}"  # 최대 100자까지만
                
                processed_documents.append(processed_doc)
            
            # DataFrame 생성 시 추가 안전장치
            try:
                self.evaluation_data = pd.DataFrame(processed_documents)
            except Exception as df_error:
                logger.error(f"DataFrame 생성 실패: {df_error}")
                # 더 안전한 방법으로 재시도
                safe_documents = []
                for doc in processed_documents:
                    safe_doc = {}
                    for key, value in doc.items():
                        # 모든 값을 문자열로 강제 변환
                        if value is None:
                            safe_doc[key] = ""
                        elif isinstance(value, (list, dict)):
                            safe_doc[key] = str(value)
                        else:
                            safe_doc[key] = str(value)
                    safe_documents.append(safe_doc)
                
                self.evaluation_data = pd.DataFrame(safe_documents)
            
            # 데이터 구조 확인
            print(f"📊 데이터 컬럼: {list(self.evaluation_data.columns)}")
            if 'evaluated_year' in self.evaluation_data.columns and 'evaluated_quarter' in self.evaluation_data.columns:
                years = self.evaluation_data['evaluated_year'].dropna().unique()
                quarters = self.evaluation_data['evaluated_quarter'].dropna().unique()
                print(f"📅 평가 기간: {years}년, Q{quarters}분기")
            elif 'quarter_key' in self.evaluation_data.columns:
                quarters = self.evaluation_data['quarter_key'].unique()
                print(f"📅 평가 분기: {quarters}")
            
            return {
                "user_id": user_id,
                "total_records": len(user_records),
                "date_range": self._extract_date_range(),
                "data_summary": self._summarize_data_structure()
            }
            
        except Exception as e:
            logger.error(f"MongoDB 데이터 로드 실패: {str(e)}")
            raise ValueError(f"MongoDB 데이터 로드 중 오류 발생: {str(e)}")
    
    def _extract_date_range(self) -> Dict[str, str]:
        """데이터의 날짜 범위를 추출합니다."""
        if self.evaluation_data is None or self.evaluation_data.empty:
            return {}
        
        date_info = {}
        
        # 다양한 날짜 필드 확인
        date_fields = ['start_date', 'end_date', 'date', 'created_at', 'updated_at', 'evaluation_date']
        
        for date_field in date_fields:
            if date_field in self.evaluation_data.columns:
                try:
                    dates = pd.to_datetime(self.evaluation_data[date_field], errors='coerce').dropna()
                    if not dates.empty:
                        date_info[f"{date_field}_min"] = dates.min().strftime('%Y-%m-%d')
                        date_info[f"{date_field}_max"] = dates.max().strftime('%Y-%m-%d')
                except:
                    continue
        
        return date_info
    
    def _summarize_data_structure(self) -> Dict[str, Any]:
        """데이터 구조를 요약합니다."""
        if self.evaluation_data is None or self.evaluation_data.empty:
            return {}
        
        try:
            summary = {
                "total_columns": len(self.evaluation_data.columns),
                "column_names": list(self.evaluation_data.columns),
                "data_types": {},
                "non_null_counts": {},
                "unique_values": {}
            }
            
            # 안전하게 데이터 타입 정보 수집
            for col in self.evaluation_data.columns:
                try:
                    summary["data_types"][col] = str(self.evaluation_data[col].dtype)
                    summary["non_null_counts"][col] = int(self.evaluation_data[col].count())
                    
                    # 문자열 컬럼의 고유값 수집 (안전하게)
                    if self.evaluation_data[col].dtype == 'object':
                        unique_count = self.evaluation_data[col].nunique()
                        if unique_count <= 20:  # 고유값이 20개 이하인 경우만
                            try:
                                summary["unique_values"][col] = self.evaluation_data[col].value_counts().to_dict()
                            except Exception:
                                summary["unique_values"][col] = {"error": "value_counts 실패"}
                except Exception as col_error:
                    logger.warning(f"컬럼 {col} 처리 실패: {col_error}")
                    summary["data_types"][col] = "unknown"
                    summary["non_null_counts"][col] = 0
            
            return summary
            
        except Exception as e:
            logger.error(f"데이터 구조 요약 실패: {e}")
            return {"error": str(e)}
    
    def analyze_user_performance(self, user_id: str) -> Dict[str, Any]:
        """사용자의 성과를 분석합니다."""
        logger.info(f"사용자 {user_id} 성과 분석 시작")
        
        if self.evaluation_data is None or self.evaluation_data.empty:
            raise ValueError("데이터가 로드되지 않았습니다.")
        
        # 기본 정보 추출
        user_info = self._extract_user_info()
        
        # 활동 데이터 분석
        activities = self._analyze_activities()
        
        # 팀 목표 관련 분석
        team_goals = self._analyze_team_goals()
        
        # 성과 지표 분석
        performance_metrics = self._analyze_performance_metrics()
        
        # 시계열 분석
        timeline_analysis = self._analyze_timeline()
        
        analysis_result = {
            "user_info": user_info,
            "activities": activities,
            "team_goals": team_goals,
            "performance_metrics": performance_metrics,
            "timeline_analysis": timeline_analysis,
            "summary": self._generate_summary(user_info, activities, team_goals, performance_metrics)
        }
        
        logger.info(f"사용자 {user_id} 성과 분석 완료")
        return analysis_result
    
    def _extract_user_info(self) -> Dict[str, Any]:
        """사용자 기본 정보를 추출합니다."""
        first_record = self.evaluation_data.iloc[0]
        
        info = {
            "user_id": first_record.get('user_id', ''),
            "name": first_record.get('name', f"User_{first_record.get('user_id', '')}"),
            "employee_number": first_record.get('employee_number', ''),
            "organization_id": first_record.get('organization_id', ''),
            "department": first_record.get('department', ''),
            "team": first_record.get('team', ''),
            "position": first_record.get('position', ''),
            "evaluation_year": first_record.get('evaluated_year', ''),  # 'evaluated_year' 필드 사용
            "evaluation_quarter": first_record.get('evaluated_quarter', ''),  # 'evaluated_quarter' 필드 사용
            "total_records": len(self.evaluation_data),
            "total_activities": first_record.get('total_activities', 0),
            "period": ""
        }
        
        # 분기별 평가 기간 설정
        if 'evaluated_year' in self.evaluation_data.columns and 'evaluated_quarter' in self.evaluation_data.columns:
            years = self.evaluation_data['evaluated_year'].dropna().unique()
            quarters = self.evaluation_data['evaluated_quarter'].dropna().unique()
            if len(years) > 0 and len(quarters) > 0:
                year_range = f"{min(years)}-{max(years)}" if len(years) > 1 else str(years[0])
                quarter_range = f"Q{min(quarters)}-Q{max(quarters)}" if len(quarters) > 1 else f"Q{quarters[0]}"
                info["period"] = f"{year_range} {quarter_range}"
        elif 'quarter_key' in self.evaluation_data.columns:
            quarters = self.evaluation_data['quarter_key'].unique()
            info["period"] = f"{', '.join(quarters)}"
        
        return info
    
    def _analyze_activities(self) -> List[Dict[str, Any]]:
        """활동 데이터를 분석합니다."""
        activities = []
        
        for idx, record in self.evaluation_data.iterrows():
            year = record.get('evaluated_year', '')
            quarter = record.get('evaluated_quarter', '')
            quarter_key = record.get('quarter_key', f'{year}Q{quarter}')
            
            # teamGoals 처리 (배열인 경우)
            if 'teamGoals_list' in record and isinstance(record['teamGoals_list'], list):
                for i, team_goal in enumerate(record['teamGoals_list']):
                    if isinstance(team_goal, dict):
                        goal_name = team_goal.get('goalName', f'목표-{i+1}')
                        contents = team_goal.get('contents', [])
                        
                        if isinstance(contents, list):
                            for j, content in enumerate(contents):
                                if isinstance(content, dict) and 'description' in content:
                                    activities.append({
                                        "description": content['description'],
                                        "date": quarter_key,
                                        "week": f"{year}년 {quarter}분기 - {goal_name}",
                                        "category": "팀목표",
                                        "source_field": "teamGoals",
                                        "record_index": idx,
                                        "goal_name": goal_name,
                                        "assigned": team_goal.get('assigned', ''),
                                        "contribution_count": team_goal.get('contributionCount', 0)
                                    })
            
            # teamGoals 문자열 처리 (리스트가 문자열로 변환된 경우)
            elif 'teamGoals' in record and isinstance(record['teamGoals'], str) and record['teamGoals'].strip():
                # 간단하게 전체 문자열을 하나의 활동으로 처리
                activities.append({
                    "description": record['teamGoals'],
                    "date": quarter_key,
                    "week": f"{year}년 {quarter}분기 팀목표",
                    "category": "팀목표",
                    "source_field": "teamGoals",
                    "record_index": idx
                })
            
            # title이나 다른 필드들도 활동으로 포함
            if 'title' in record and record['title']:
                activities.append({
                    "description": f"리포트: {record['title']}",
                    "date": quarter_key,
                    "week": f"{year}년 {quarter}분기 리포트",
                    "category": "성과리포트",
                    "source_field": "title",
                    "record_index": idx
                })
        
        return activities
    
    def _analyze_team_goals(self) -> List[Dict[str, Any]]:
        """팀 목표 관련 데이터를 분석합니다."""
        team_goals = []
        
        # 팀 목표 관련 필드 찾기
        goal_fields = ['team_goal', 'goal', 'objective', 'target', 'kpi', 'performance_indicator']
        
        for field in goal_fields:
            if field in self.evaluation_data.columns:
                unique_goals = self.evaluation_data[field].dropna().unique()
                
                for goal in unique_goals:
                    if isinstance(goal, str) and goal.strip():
                        # 해당 목표와 관련된 활동 수 계산
                        related_activities = self._count_related_activities(goal)
                        
                        team_goals.append({
                            "goal_name": goal.strip(),
                            "source_field": field,
                            "related_activities": related_activities,
                            "assigned": "배정" if related_activities > 0 else "미배정"
                        })
        
        return team_goals
    
    def _analyze_performance_metrics(self) -> Dict[str, Any]:
        """성과 지표를 분석합니다."""
        metrics = {
            "total_activities": len(self._analyze_activities()),
            "active_weeks": 0,
            "productivity_score": 0,
            "goal_achievement": 0
        }
        
        # 활성 주차 계산
        if 'start_date' in self.evaluation_data.columns:
            unique_weeks = self.evaluation_data['start_date'].dropna().nunique()
            metrics["active_weeks"] = unique_weeks
        
        # 생산성 점수 (활동 수 / 활성 주차)
        if metrics["active_weeks"] > 0:
            metrics["productivity_score"] = round(metrics["total_activities"] / metrics["active_weeks"], 2)
        
        # 목표 달성률 (배정된 목표 비율)
        team_goals = self._analyze_team_goals()
        if team_goals:
            assigned_goals = sum(1 for goal in team_goals if goal["assigned"] == "배정")
            metrics["goal_achievement"] = round((assigned_goals / len(team_goals)) * 100, 1)
        
        return metrics
    
    def _analyze_timeline(self) -> List[Dict[str, Any]]:
        """시계열 분석을 수행합니다."""
        timeline = []
        
        if 'start_date' in self.evaluation_data.columns:
            # 날짜별로 그룹화
            date_groups = self.evaluation_data.groupby('start_date')
            
            for date, group in date_groups:
                if pd.notna(date):
                    activities = []
                    for _, record in group.iterrows():
                        if 'done_task' in record and pd.notna(record['done_task']):
                            activities.append(record['done_task'])
                    
                    timeline.append({
                        "date": str(date),
                        "week": self._convert_date_to_week(str(date), str(record.get('end_date', ''))),
                        "activity_count": len(activities),
                        "activities": activities
                    })
        
        return sorted(timeline, key=lambda x: x['date'])
    
    
    def _count_related_activities(self, goal: str) -> int:
        """특정 목표와 관련된 활동 수를 계산합니다."""
        if 'done_task' not in self.evaluation_data.columns:
            return 0
        
        goal_keywords = goal.lower().split()
        count = 0
        
        for task in self.evaluation_data['done_task'].dropna():
            task_lower = str(task).lower()
            # 목표의 주요 키워드가 업무에 포함되어 있는지 확인
            if any(keyword in task_lower for keyword in goal_keywords if len(keyword) > 2):
                count += 1
        
        return count
    
    def _convert_date_to_week(self, start_date: str, end_date: str) -> str:
        """날짜를 'N월 N주차' 형식으로 변환합니다."""
        try:
            if not start_date or start_date == '':
                return "날짜 정보 없음"
            
            from datetime import datetime
            start_dt = datetime.strptime(str(start_date), '%Y-%m-%d')
            month = start_dt.month
            
            # 해당 월의 첫 번째 날
            first_day = datetime(start_dt.year, month, 1)
            first_weekday = first_day.weekday()
            
            # 주차 계산
            days_from_first = (start_dt - first_day).days
            week_number = (days_from_first + first_weekday) // 7 + 1
            
            return f"{month}월 {week_number}주차"
        except:
            return f"주간 보고서 ({start_date})"
    
    def _generate_summary(self, user_info: Dict, activities: List, team_goals: List, metrics: Dict) -> Dict[str, Any]:
        """분석 결과를 요약합니다."""
        return {
            "overview": f"{user_info['name']}님의 {user_info['period']} 기간 성과 분석",
            "key_metrics": {
                "총 활동 수": metrics["total_activities"],
                "활성 주차": metrics["active_weeks"],
                "주간 평균 활동": metrics["productivity_score"],
                "목표 달성률": f"{metrics['goal_achievement']}%"
            },
            "activity_categories": self._summarize_activity_categories(activities),
            "goal_status": {
                "총 목표 수": len(team_goals),
                "배정된 목표": sum(1 for goal in team_goals if goal["assigned"] == "배정"),
                "미배정 목표": sum(1 for goal in team_goals if goal["assigned"] == "미배정")
            }
        }
    
    def _summarize_activity_categories(self, activities: List) -> Dict[str, int]:
        """활동 카테고리별 집계를 생성합니다."""
        categories = {}
        for activity in activities:
            category = activity.get("category", "기타")
            categories[category] = categories.get(category, 0) + 1
        return categories

    def generate_evaluation_prompt(self, analysis_data: Dict[str, Any]) -> str:
        """평가용 프롬프트를 생성합니다."""
        user_info = analysis_data["user_info"]
        activities = analysis_data["activities"]
        team_goals = analysis_data["team_goals"]
        
        # 🔧 수정된 부분: MariaDB에서 실제 사용자 이름 조회
        real_employee_name = self.get_user_name_from_mariadb(user_info['user_id'])
        
        # 분기별 활동 정리
        quarterly_activities = {}
        for activity in activities:
            quarter_key = activity.get('date', '')
            if quarter_key not in quarterly_activities:
                quarterly_activities[quarter_key] = []
            quarterly_activities[quarter_key].append(activity)
        
        prompt = f"""
당신은 전문적인 HR 평가 에이전트입니다. MongoDB에서 추출한 데이터를 기반으로 직원의 연간 성과를 분기별로 분석하여 객관적인 평가를 수행해주세요.

## 평가 대상 정보

### 직원 기본 정보
- 이름: {real_employee_name}
- 사용자 ID: {user_info['user_id']}
- 평가 기간: {user_info['period']}
- 총 활동 기록: {user_info['total_records']}건

### 분기별 활동 현황
"""
        
        # 분기별 활동 목록 추가
        for quarter_key, quarter_activities in quarterly_activities.items():
            prompt += f"\n**{quarter_key}**\n"
            for activity in quarter_activities:
                prompt += f"- {activity['description']}\n"
        
        # 팀 목표 추가
        if team_goals:
            prompt += "\n### 연간 팀 목표 현황\n"
            for i, goal in enumerate(team_goals, 1):
                prompt += f"**목표 {i}**: {goal['goal_name']} (관련 활동: {goal['related_activities']}건)\n"
        
        prompt += f"""

## 평가 결과 형식

다음 JSON 형식으로 분기별 성과 평가를 제공해주세요:

```json
{{
"user_id": "{user_info['user_id']}",
"employee_name": "{real_employee_name}",
"evaluation_year": "2024",
"quarterlyPerformance": [
    {{
    "quarter": "1분기",
    "rating": "1st|2nd|3rd|4th",
    "summary": "1분기 성과 요약"
    }},
    {{
    "quarter": "2분기", 
    "rating": "1st|2nd|3rd|4th",
    "summary": "2분기 성과 요약"
    }},
    {{
    "quarter": "3분기",
    "rating": "1st|2nd|3rd|4th", 
    "summary": "3분기 성과 요약"
    }},
    {{
    "quarter": "4분기",
    "rating": "1st|2nd|3rd|4th",
    "summary": "4분기 성과 요약"
    }}
],
"keyAchievements": [
    "연간 주요 성과 1",
    "연간 주요 성과 2", 
    "연간 주요 성과 3"
],
"overall_assessment": {{
    "annual_rating": "우수|양호|보통|미흡|부족",
    "total_activities": {len(activities)},
    "evaluation_period": "{user_info['period']}",
    "strengths": [
    "강점 1",
    "강점 2"
    ],
    "improvement_areas": [
    "개선점 1", 
    "개선점 2"
    ]
}}
}}
```

## 평가 가이드라인

1. **분기별 상대평가 (quarterlyPerformance) **:
- 각 등급은 4개 분기에 정확히 하나씩만 배정되어야 합니다.
- rating: 
    1st(최우수): 가장 뛰어난 성과를 보인 1개 분기
    2nd(우수): 두 번째로 우수한 성과를 보인 1개 분기
    3rd(양호): 세 번째로 좋은 성과를 보인 1개 분기
    4th(보통): 상대적으로 가장 아쉬운 성과를 보인 1개 분기
    등급 중복은 허용되지 않습니다. (예: 2nd가 여러 번 등장하면 안 됨)
- summary: 해당 분기의 주요 활동과 성과를 2-3문장으로 요약 (한국어)
- 실제 활동 데이터를 기반으로 객관적 평가

2. **연간 주요 성과 (keyAchievements)**:
- 1년 전체 활동을 종합하여 3-5개의 핵심 성과 도출
- 구체적인 수치나 결과가 포함된 성과 위주로 작성
- 모두 한국어로 작성

3. **전체 평가 (overall_assessment)**:
- annual_rating: 연간 종합 평가 등급
- strengths: 직원의 주요 강점 2-3개
- improvement_areas: 향후 개선이 필요한 영역 2-3개

4. **작성 원칙**:
- 모든 텍스트는 한국어로 작성
- 실제 활동 데이터에 기반한 객관적 평가
- 구체적이고 건설적인 피드백 제공

JSON 형식을 정확히 준수하여 응답해주세요.
"""
        
        return prompt
    
    def close_connection(self):
        """MongoDB와 MariaDB 연결을 종료합니다."""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB 연결 종료")
        
        if self.mariadb_connection:
            self.mariadb_connection.close()
            logger.info("MariaDB 연결 종료")
    
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
                        "content": "당신은 전문적인 HR 평가 에이전트입니다. MongoDB 데이터를 기반으로 객관적이고 구체적인 성과 평가를 제공하며, 항상 정확한 JSON 형식으로 응답합니다."
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
                               analysis_data: Dict[str, Any],
                               filename: Optional[str] = None) -> str:
        """평가 결과를 JSON 파일로 저장합니다."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            user_id = analysis_data.get("user_info", {}).get("user_id", "unknown")
            filename = f"performance_evaluation_{user_id}_{timestamp}.json"
        
        # user_id가 결과에 포함되어 있는지 확인하고 없으면 추가
        if "user_id" not in results:
            results["user_id"] = analysis_data.get("user_info", {}).get("user_id", "unknown")
        
        # 최종 결과 구성 - 요청된 형태로 단순화
        final_results = {
            **results,  # 평가 결과를 최상위에 배치
            "metadata": {
                "evaluation_timestamp": datetime.now().isoformat(),
                "data_source": "MongoDB",
                "database": self.mongodb_config['database'],
                "collection": self.mongodb_config['collection'],
                "ai_model": self.model,
                "total_records_analyzed": len(self.evaluation_data) if self.evaluation_data is not None else 0,
                "config_source": "환경변수"
            },
            "source_data_summary": {
                "total_activities_found": len(analysis_data.get("activities", [])),
                "evaluation_period": analysis_data.get("user_info", {}).get("period", ""),
                "data_quality": "정상" if len(analysis_data.get("activities", [])) > 0 else "활동 데이터 부족"
            }
        }
        
        output_file = self.output_path / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"평가 결과 저장 완료: {output_file}")
        return str(output_file)
    
    def execute_complete_evaluation(self, user_id: str) -> Dict[str, Any]:
        """사용자에 대한 완전한 평가를 실행합니다."""
        logger.info(f"=== 사용자 {user_id} 완전 평가 시작 ===")
        
        try:
            # 1단계: MongoDB 연결
            self.connect_to_mongodb()
            
            # 2단계: 사용자 데이터 로드
            load_result = self.load_user_data_from_mongodb(user_id)
            
            # 3단계: 데이터 분석
            analysis_data = self.analyze_user_performance(user_id)
            
            # 4단계: 프롬프트 생성
            prompt = self.generate_evaluation_prompt(analysis_data)
            
            # 5단계: LLM 평가 실행
            evaluation_result = self.execute_llm_evaluation(prompt)
            
            # 6단계: 결과 저장
            if "error" not in evaluation_result:
                output_file = self.save_evaluation_results(
                    evaluation_result,
                    analysis_data,
                    f"performance_evaluation_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                
                # 평가 이력에 추가
                self.evaluation_history.append({
                    "user_id": user_id,
                    "timestamp": datetime.now().isoformat(),
                    "output_file": output_file,
                    "status": "success"
                })
                
                logger.info(f"사용자 {user_id} 평가 완료 - 성공")
                return {
                    "success": True,
                    "evaluation_result": evaluation_result,
                    "analysis_data": analysis_data,
                    "output_file": output_file
                }
            else:
                # 오류 발생 시 결과
                error_result = {
                    "user_id": user_id,
                    "timestamp": datetime.now().isoformat(),
                    "error": evaluation_result["error"],
                    "analysis_data": analysis_data
                }
                
                output_file = self.save_evaluation_results(
                    error_result,
                    analysis_data,
                    f"evaluation_error_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                
                self.evaluation_history.append({
                    "user_id": user_id,
                    "timestamp": datetime.now().isoformat(),
                    "output_file": output_file,
                    "status": "failed"
                })
                
                logger.info(f"사용자 {user_id} 평가 실패")
                return {
                    "success": False,
                    "error": evaluation_result["error"],
                    "analysis_data": analysis_data,
                    "output_file": output_file
                }
                
        except Exception as e:
            logger.error(f"사용자 {user_id} 평가 실패: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "user_id": user_id
            }
    
    def execute_batch_evaluation(self, target_user_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """다수 사용자에 대한 배치 평가를 실행합니다."""
        logger.info("=== 배치 평가 시작 ===")
        
        try:
            # 대상 사용자 목록 결정
            if target_user_ids is None:
                target_user_ids = self.get_available_user_ids()
                logger.info(f"MongoDB에서 발견된 사용자 ID: {target_user_ids}")
            
            if not target_user_ids:
                return {
                    "batch_metadata": {
                        "start_time": datetime.now().isoformat(),
                        "error": "사용 가능한 사용자 ID가 없습니다."
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
                    "data_source": f"MongoDB: {self.mongodb_config['database']}.{self.mongodb_config['collection']}",
                    "config_source": "환경변수"
                },
                "individual_results": {},
                "batch_summary": {
                    "successful_evaluations": 0,
                    "failed_evaluations": 0,
                    "successful_users": [],
                    "failed_users": []
                }
            }
            
            # 개별 사용자 평가 실행
            for user_id in target_user_ids:
                logger.info(f"배치 평가 진행 중: {user_id}")
                
                try:
                    result = self.execute_complete_evaluation(user_id)
                    
                    if result.get("success", False):
                        batch_results["batch_summary"]["successful_evaluations"] += 1
                        batch_results["batch_summary"]["successful_users"].append(user_id)
                        batch_results["individual_results"][user_id] = {
                            "status": "success",
                            "output_file": result.get("output_file", ""),
                            "evaluation_summary": result.get("evaluation_result", {}).get("overall_assessment", {})
                        }
                    else:
                        batch_results["batch_summary"]["failed_evaluations"] += 1
                        batch_results["batch_summary"]["failed_users"].append(user_id)
                        batch_results["individual_results"][user_id] = {
                            "status": "failed",
                            "error": result.get("error", "알 수 없는 오류")
                        }
                        
                except Exception as e:
                    logger.error(f"사용자 {user_id} 배치 평가 실패: {str(e)}")
                    batch_results["batch_summary"]["failed_evaluations"] += 1
                    batch_results["batch_summary"]["failed_users"].append(user_id)
                    batch_results["individual_results"][user_id] = {
                        "status": "failed",
                        "error": str(e)
                    }
            
            batch_results["batch_metadata"]["end_time"] = datetime.now().isoformat()
            
            # 배치 결과 저장
            batch_output_file = self.save_evaluation_results(
                batch_results,
                {"batch_evaluation": True},
                f"batch_performance_evaluation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            
            logger.info(f"배치 평가 완료 - 성공: {batch_results['batch_summary']['successful_evaluations']}명, "
                       f"실패: {batch_results['batch_summary']['failed_evaluations']}명")
            
            return batch_results
            
        except Exception as e:
            logger.error(f"배치 평가 실패: {str(e)}")
            return {
                "batch_metadata": {
                    "start_time": datetime.now().isoformat(),
                    "error": str(e)
                },
                "batch_summary": {
                    "successful_evaluations": 0,
                    "failed_evaluations": len(target_user_ids) if target_user_ids else 0
                }
            }
    
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
    
    print("🎯 === MongoDB 기반 주간 보고서 평가 시스템 ===")
    print("📊 MongoDB 전용 AI 평가 시스템 (환경변수 버전)")
    
    try:
        print(f"\n🤖 시스템 초기화 중...")
        print("📋 환경변수 검증 중...")
        
        # 에이전트 초기화 (환경변수 자동 로드)
        agent = MongoDBWeeklyReportAgent()
        
        print("✅ 시스템 초기화 완료!")
        
        # 사용자 메뉴
        while True:
            print(f"\n🎯 === 메인 메뉴 ===")
            print("1. MongoDB 연결 테스트")
            print("2. 모든 사용자 목록 조회")
            print("3. 단일 사용자 평가")
            print("4. 배치 평가 (모든 사용자)")
            print("5. 평가 통계 확인")
            print("6. 종료")
            
            choice = input("\n선택하세요 (1-6): ").strip()
            
            if choice == "1":
                print("\n🔗 MongoDB 연결 테스트 중...")
                try:
                    agent.connect_to_mongodb()
                    print("✅ MongoDB 연결 성공!")
                    
                    # 컬렉션 정보 출력
                    doc_count = agent.collection.count_documents({})
                    print(f"📊 총 문서 수: {doc_count}개")
                except Exception as e:
                    print(f"❌ MongoDB 연결 실패: {e}")
    except Exception as e:
        print(f"\n🤖 시스템 초기화 실패")