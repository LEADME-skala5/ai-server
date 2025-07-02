#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주간 보고서 분기별 배치 평가 시스템 - 환경변수 버전
실행: python main.py
"""

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
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =====================================
# 설정 클래스
class Config:
    def __init__(self):
        # 환경변수에서 API 키 로드
        self.OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
        self.PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
        
        # 모델 및 기본 설정
        self.MODEL = os.getenv('OPENAI_MODEL', 'gpt-4-turbo')
        self.OUTPUT_PATH = os.getenv('OUTPUT_PATH', './output')
        
        # Pinecone 설정
        self.PINECONE_INDEX_NAME = os.getenv('PINECONE_INDEX_NAME', 'skore')
        
        # MariaDB 설정 - 환경변수에서 로드
        self.DB_CONFIG = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'charset': os.getenv('DB_CHARSET', 'utf8mb4')
        }
        
        # 필수 환경변수 검증
        self._validate_config()
    
    def _validate_config(self):
        """필수 환경변수 검증"""
        required_vars = {
            'OPENAI_API_KEY': self.OPENAI_API_KEY,
            'PINECONE_API_KEY': self.PINECONE_API_KEY,
            'DB_HOST': self.DB_CONFIG['host'],
            'DB_USER': self.DB_CONFIG['user'],
            'DB_PASSWORD': self.DB_CONFIG['password'],
            'DB_NAME': self.DB_CONFIG['database']
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        
        if missing_vars:
            raise ValueError(
                f"필수 환경변수가 설정되지 않았습니다: {', '.join(missing_vars)}\n"
                f".env 파일을 확인하세요."
            )
        
        logger.info("✅ 모든 필수 환경변수가 정상적으로 로드되었습니다.")

# =====================================
# 데이터베이스 관리자
class DatabaseManager:
    def __init__(self, db_config):
        self.db_config = db_config
    
    def connect(self):
        """MariaDB 연결"""
        try:
            connection = pymysql.connect(**self.db_config)
            logger.info("MariaDB 연결 성공")
            return connection
        except Exception as e:
            logger.error(f"MariaDB 연결 실패: {str(e)}")
            raise
    
    def load_team_data(self):
        """팀 기준 및 목표 데이터 로드"""
        logger.info("MariaDB에서 팀 데이터 로드 시작")
        
        connection = self.connect()
        try:
            # team_criteria 테이블 로드
            criteria_query = "SELECT * FROM team_criteria"
            team_criteria = pd.read_sql(criteria_query, connection)
            logger.info(f"team_criteria 로드 완료: {len(team_criteria)}건")
            
            # team_goal 테이블 로드
            goals_query = "SELECT * FROM team_goal"
            team_goals = pd.read_sql(goals_query, connection)
            logger.info(f"team_goal 로드 완료: {len(team_goals)}건")
            
            return team_criteria, team_goals
            
        finally:
            connection.close()
    
    def get_organization_name(self, organization_id):
        """organization_id로 조직명 조회 - 실제 컬럼명 사용"""
        if not organization_id:
            return "미지정 팀"
            
        logger.info(f"조직 ID {organization_id}의 이름 조회 시작")
        
        connection = self.connect()
        try:
            # 먼저 organizations 테이블 구조 확인
            cursor = connection.cursor()
            cursor.execute("DESCRIBE organizations")
            columns = [row[0] for row in cursor.fetchall()]
            logger.info(f"organizations 테이블 컬럼: {columns}")
            
            # 가능한 ID 컬럼명들 시도
            possible_id_columns = ['id', 'organization_id', 'org_id', 'pk']
            id_column = None
            
            for col in possible_id_columns:
                if col in columns:
                    id_column = col
                    break
            
            if not id_column:
                logger.error(f"organizations 테이블에서 ID 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {columns}")
                return f"조직_{organization_id}"
            
            # 가능한 이름 컬럼명들 시도
            possible_name_columns = ['name', 'org_name', 'organization_name', 'dept_name', 'team_name']
            name_column = None
            
            for col in possible_name_columns:
                if col in columns:
                    name_column = col
                    break
            
            if not name_column:
                logger.error(f"organizations 테이블에서 이름 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {columns}")
                return f"조직_{organization_id}"
            
            # 실제 조직명 조회
            query = f"SELECT {name_column} FROM organizations WHERE {id_column} = %s"
            result = pd.read_sql(query, connection, params=[str(organization_id)])
            
            if not result.empty and not result[name_column].isna().iloc[0]:
                org_name = result[name_column].iloc[0]
                logger.info(f"조직 ID {organization_id}의 이름: {org_name}")
                return str(org_name).strip()
            else:
                logger.warning(f"조직 ID {organization_id}에 해당하는 이름을 찾을 수 없습니다.")
                return f"조직_{organization_id}"
                
        except Exception as e:
            logger.error(f"조직명 조회 실패 (ID: {organization_id}): {e}")
            return f"조직_{organization_id}"
        finally:
            connection.close()

# =====================================
# Pinecone 관리자
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

# =====================================
# 데이터 처리기
class DataProcessor:
    @staticmethod
    def convert_pinecone_to_dataframe(search_results):
        """Pinecone 검색 결과를 DataFrame으로 변환"""
        weekly_records = []
        
        for match in search_results.matches:
            metadata = match.metadata
            
            record = {
                'employee_number': metadata.get('user_id'),
                'name': f"User_{metadata.get('user_id')}",
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
        
        df = pd.DataFrame(weekly_records)
        
        # 중복 제거
        if not df.empty:
            df = df.drop_duplicates(
                subset=['employee_number', 'start_date', 'end_date'], 
                keep='first'
            )
        
        return df
    
    @staticmethod
    def split_data_by_quarters(employee_data):
        """직원 데이터를 분기별로 분할"""
        if employee_data.empty:
            return {}
        
        # 날짜 컬럼을 datetime으로 변환
        employee_data['start_date_dt'] = pd.to_datetime(employee_data['start_date'], errors='coerce')
        employee_data['end_date_dt'] = pd.to_datetime(employee_data['end_date'], errors='coerce')
        
        # 유효한 날짜가 있는 데이터만 필터링
        valid_data = employee_data.dropna(subset=['start_date_dt'])
        
        if valid_data.empty:
            logger.warning("유효한 날짜 데이터가 없습니다.")
            return {}
        
        # 분기별 데이터 분할
        quarters = {}
        
        for _, row in valid_data.iterrows():
            start_date = row['start_date_dt']
            year = start_date.year
            month = start_date.month
            
            # 분기 계산 (1-3월: Q1, 4-6월: Q2, 7-9월: Q3, 10-12월: Q4)
            if 1 <= month <= 3:
                quarter = 1
                quarter_start = f"{year}-01-01"
                quarter_end = f"{year}-03-31"
            elif 4 <= month <= 6:
                quarter = 2
                quarter_start = f"{year}-04-01"
                quarter_end = f"{year}-06-30"
            elif 7 <= month <= 9:
                quarter = 3
                quarter_start = f"{year}-07-01"
                quarter_end = f"{year}-09-30"
            else:  # 10-12월
                quarter = 4
                quarter_start = f"{year}-10-01"
                quarter_end = f"{year}-12-31"
            
            quarter_key = f"{year}_Q{quarter}"
            
            if quarter_key not in quarters:
                quarters[quarter_key] = {
                    'data': [],
                    'year': year,
                    'quarter': quarter,
                    'start_date': quarter_start,
                    'end_date': quarter_end
                }
            
            quarters[quarter_key]['data'].append(row.to_dict())
        
        # DataFrame으로 변환
        for quarter_key in quarters:
            quarters[quarter_key]['dataframe'] = pd.DataFrame(quarters[quarter_key]['data'])
        
        logger.info(f"데이터를 {len(quarters)}개 분기로 분할: {list(quarters.keys())}")
        return quarters
    
    @staticmethod
    def extract_employee_info_for_quarter(quarter_data, year, quarter):
        """분기별 직원 정보 추출"""
        if quarter_data.empty:
            return None
            
        info = {
            "name": quarter_data['name'].iloc[0] if 'name' in quarter_data.columns else f"User_{quarter_data['employee_number'].iloc[0]}",
            "employee_number": quarter_data['employee_number'].iloc[0],
            "organization_id": quarter_data['organization_id'].iloc[0] if 'organization_id' in quarter_data.columns else "",
            "evaluation_year": year,
            "evaluation_quarter": quarter,
            "total_weeks": len(quarter_data),
            "total_activities": len(quarter_data)
        }
        
        # 분기별 날짜 범위 설정
        if 'start_date' in quarter_data.columns and 'end_date' in quarter_data.columns:
            start_dates = pd.to_datetime(quarter_data['start_date'], errors='coerce').dropna()
            end_dates = pd.to_datetime(quarter_data['end_date'], errors='coerce').dropna()
            if not start_dates.empty and not end_dates.empty:
                info["period"] = f"{start_dates.min().strftime('%Y-%m-%d')} ~ {end_dates.max().strftime('%Y-%m-%d')}"
        
        # 주간 업무 데이터 추가
        info['weekly_tasks'] = quarter_data[['start_date', 'end_date', 'done_task']].to_dict('records')
        
        return info
    
    @staticmethod
    def filter_team_data_by_org(team_data, org_id, org_keywords):
        """조직 ID로 팀 데이터 필터링"""
        if team_data is None or team_data.empty or not org_id:
            return team_data.to_dict('records') if team_data is not None else []
        
        # organization_id 컬럼 찾기
        org_column = None
        for col in team_data.columns:
            col_lower = str(col).lower().strip()
            if any(keyword.lower() in col_lower for keyword in org_keywords):
                org_column = col
                break
        
        if org_column:
            filtered_data = team_data[
                team_data[org_column].astype(str) == str(org_id)
            ].to_dict('records')
            logger.info(f"팀 데이터 필터링 완료: {len(filtered_data)}개")
            return filtered_data
        
        logger.warning("조직 ID 매칭 실패, 전체 데이터 반환")
        return team_data.to_dict('records')

# =====================================
# LLM 평가기
class LLMEvaluator:
    def __init__(self, api_key, model="gpt-4-turbo"):
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model
    
    def generate_prompt(self, employee_data, team_goals, team_criteria):
        """평가용 프롬프트 생성"""
        team_categories = self._extract_goal_categories(team_goals)
        
        prompt = f"""
당신은 전문적인 HR 평가 에이전트입니다. 직원의 주간 보고서를 종합 분석하여 객관적인 성과 평가를 수행해주세요.

## 평가 대상 정보
- 이름: {employee_data['name']}
- 직원번호: {employee_data['employee_number']}
- 조직 ID: {employee_data['organization_id']}
- 평가 기간: {employee_data.get('period', 'N/A')}
- 총 평가 주차: {employee_data['total_weeks']}주

## 주간별 수행 업무
"""
        
        # 주간별 업무 추가
        weekly_tasks = employee_data.get('weekly_tasks', [])
        for i, task in enumerate(weekly_tasks, 1):
            start_date = task.get('start_date', 'N/A')
            end_date = task.get('end_date', 'N/A')
            done_task = task.get('done_task', 'N/A')
            prompt += f"\n**{i}주차 ({start_date} ~ {end_date})**\n{done_task}\n"
        
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

## 팀 목표
"""
        for i, category in enumerate(team_categories, 1):
            prompt += f"{i}. {category}\n"
        
        prompt += "\nJSON 형식을 정확히 준수하여 응답해주세요."
        
        return prompt
    
    def _extract_goal_categories(self, team_goals):
        """팀 목표에서 카테고리 추출 - RDB 데이터 기반"""
        if not team_goals:
            logger.warning("팀 목표 데이터가 없습니다. 빈 목표 리스트를 반환합니다.")
            return []
        
        logger.info(f"팀 목표 데이터 구조 분석 시작 - 총 {len(team_goals)}개 레코드")
        
        # 첫 번째 레코드로 컬럼 구조 확인
        if team_goals:
            first_record = team_goals[0]
            logger.info(f"팀 목표 데이터 컬럼: {list(first_record.keys())}")
        
        # 목표명/과제명을 찾기 위한 키워드 (우선순위 순)
        goal_keywords = [
            'goal_name',        # 목표명
            'task_name',        # 과제명  
            'objective_name',   # 목적명
            'kpi_name',         # KPI명
            '성과지표명',        # 한글 성과지표명
            '과제명',           # 한글 과제명
            '목표명',           # 한글 목표명
            'objective',        # 목적
            'goal',            # 목표
            'task'             # 과제
        ]
        
        # 실제 존재하는 컬럼과 매칭
        goal_key = None
        for keyword in goal_keywords:
            # 정확한 매칭 우선
            if keyword in first_record:
                goal_key = keyword
                logger.info(f"목표 컬럼 발견 (정확한 매칭): {goal_key}")
                break
        
        # 정확한 매칭이 없으면 부분 매칭 시도
        if not goal_key:
            for keyword in goal_keywords:
                for actual_key in first_record.keys():
                    if keyword.lower() in str(actual_key).lower():
                        goal_key = actual_key
                        logger.info(f"목표 컬럼 발견 (부분 매칭): {goal_key} (키워드: {keyword})")
                        break
                if goal_key:
                    break
        
        if not goal_key:
            logger.error(f"목표 관련 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {list(first_record.keys())}")
            return []
        
        # 목표 카테고리 추출
        categories = []
        for record in team_goals:
            goal_value = record.get(goal_key)
            if goal_value:
                goal_str = str(goal_value).strip()
                # 유효한 목표명인지 확인
                if (goal_str and 
                    goal_str.lower() not in ['nan', 'null', 'none', ''] and
                    len(goal_str) > 1):  # 최소 2글자 이상
                    categories.append(goal_str)
        
        # 중복 제거 및 정렬
        unique_categories = sorted(list(set(categories)))
        
        logger.info(f"추출된 팀 목표 카테고리 ({len(unique_categories)}개): {unique_categories}")
        
        if not unique_categories:
            logger.warning("유효한 팀 목표를 추출하지 못했습니다.")
            
        return unique_categories
    
    def execute_evaluation(self, prompt):
        """LLM 평가 실행"""
        try:
            logger.info(f"LLM 평가 실행 - 모델: {self.model}")
            print(f"🤖 OpenAI API 호출 중...")
            
            response = self.client.chat.completions.create(
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
            print(f"✅ OpenAI API 응답 수신 완료")
            
            # JSON 추출 및 파싱
            json_text = self._extract_json(response_text)
            result = json.loads(json_text)
            
            logger.info("LLM 평가 완료")
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
    
    def _extract_json(self, response_text):
        """응답에서 JSON 부분 추출"""
        if "```json" in response_text:
            json_start = response_text.find("```json") + 7
            json_end = response_text.find("```", json_start)
            return response_text[json_start:json_end].strip()
        else:
            return response_text.strip()

# =====================================
# 메인 평가 클래스
class WeeklyReportEvaluator:
    def __init__(self, config=None):
        self.config = config or Config()
        
        # 컴포넌트 초기화
        print("📊 데이터베이스 연결 중...")
        self.db_manager = DatabaseManager(self.config.DB_CONFIG)
        
        print("🔍 Pinecone 연결 중...")
        self.pinecone_manager = PineconeManager(
            self.config.PINECONE_API_KEY, 
            self.config.PINECONE_INDEX_NAME
        )
        
        print("🤖 LLM 초기화 중...")
        self.llm_evaluator = LLMEvaluator(
            self.config.OPENAI_API_KEY, 
            self.config.MODEL
        )
        
        # 출력 디렉토리 생성
        self.output_path = Path(self.config.OUTPUT_PATH)
        self.output_path.mkdir(exist_ok=True)
        
        logger.info("WeeklyReportEvaluator 초기화 완료")
    
    def evaluate_single_user(self, user_id):
        """단일 사용자 평가 - 분기별로 분할"""
        logger.info(f"사용자 {user_id} 분기별 평가 시작")
        
        try:
            # 1. 데이터 로드
            team_criteria, team_goals = self.db_manager.load_team_data()
            search_results = self.pinecone_manager.search_user_data(user_id)
            
            if not search_results.matches:
                raise ValueError(f"사용자 {user_id}의 데이터가 없습니다.")
            
            # 2. 데이터 처리 및 분기별 분할
            weekly_data = DataProcessor.convert_pinecone_to_dataframe(search_results)
            quarterly_data = DataProcessor.split_data_by_quarters(weekly_data)
            
            if not quarterly_data:
                raise ValueError(f"사용자 {user_id}의 유효한 분기별 데이터가 없습니다.")
            
            # 3. 각 분기별로 평가 수행
            quarterly_results = {}
            
            for quarter_key, quarter_info in quarterly_data.items():
                print(f"📊 {quarter_key} 평가 중...")
                
                quarter_df = quarter_info['dataframe']
                year = quarter_info['year']
                quarter = quarter_info['quarter']
                
                # 분기별 직원 정보 추출
                employee_info = DataProcessor.extract_employee_info_for_quarter(
                    quarter_df, year, quarter
                )
                
                if not employee_info:
                    logger.warning(f"사용자 {user_id}의 {quarter_key} 데이터 처리 실패")
                    continue
                
                # 팀 데이터 필터링
                org_keywords = ['organization_id', 'org_id', 'team_id', '조직', '팀']
                filtered_goals = DataProcessor.filter_team_data_by_org(
                    team_goals, employee_info['organization_id'], org_keywords
                )
                filtered_criteria = DataProcessor.filter_team_data_by_org(
                    team_criteria, employee_info['organization_id'], org_keywords
                )
                
                # LLM 평가
                prompt = self.llm_evaluator.generate_prompt(
                    employee_info, filtered_goals, filtered_criteria
                )
                evaluation_result = self.llm_evaluator.execute_evaluation(prompt)
                
                # 분기별 결과 저장
                if "error" not in evaluation_result:
                    output_file = self._save_quarterly_results(
                        evaluation_result, user_id, employee_info, year, quarter,
                        quarter_info['start_date'], quarter_info['end_date']
                    )
                    quarterly_results[quarter_key] = {
                        "status": "success",
                        "output_file": output_file,
                        "year": year,
                        "quarter": quarter
                    }
                    print(f"✅ {quarter_key} 평가 완료")
                else:
                    quarterly_results[quarter_key] = {
                        "status": "failed",
                        "error": evaluation_result.get("error"),
                        "year": year,
                        "quarter": quarter
                    }
                    print(f"❌ {quarter_key} 평가 실패: {evaluation_result.get('error')}")
            
            logger.info(f"사용자 {user_id} 분기별 평가 완료 - {len(quarterly_results)}개 분기")
            return {
                "user_id": user_id,
                "quarterly_results": quarterly_results,
                "total_quarters": len(quarterly_results),
                "successful_quarters": len([r for r in quarterly_results.values() if r["status"] == "success"])
            }
                
        except Exception as e:
            logger.error(f"사용자 {user_id} 평가 실패: {e}")
            return {"error": str(e), "user_id": user_id}
    
    def evaluate_batch_users(self, user_ids=None):
        """배치 사용자 평가 - 분기별 처리"""
        logger.info("배치 분기별 평가 시작")
        
        if user_ids is None:
            user_ids = self.pinecone_manager.get_available_user_ids()
        
        if not user_ids:
            return {"error": "사용 가능한 사용자가 없습니다."}
        
        # 수정된 batch_results 구조
        batch_results = {
            "batch_metadata": {
                "start_time": datetime.now().isoformat(),
                "target_user_ids": user_ids,
                "total_users": len(user_ids)
            },
            "individual_results": {},  # 이 키가 누락되어 있었음
            "batch_summary": {
                "successful_users": 0,
                "failed_users": 0,
                "total_quarters_processed": 0,
                "successful_quarters": 0,
                "failed_quarters": 0
            }
        }
        
        for i, user_id in enumerate(user_ids, 1):
            print(f"\n📊 배치 평가 진행: {i}/{len(user_ids)} - User {user_id}")
            
            result = self.evaluate_single_user(user_id)
            batch_results["individual_results"][user_id] = result
            
            if "error" not in result:
                batch_results["batch_summary"]["successful_users"] += 1
                batch_results["batch_summary"]["total_quarters_processed"] += result.get("total_quarters", 0)
                batch_results["batch_summary"]["successful_quarters"] += result.get("successful_quarters", 0)
                batch_results["batch_summary"]["failed_quarters"] += (
                    result.get("total_quarters", 0) - result.get("successful_quarters", 0)
                )
                
                print(f"✅ User {user_id} 평가 성공 - {result.get('successful_quarters', 0)}/{result.get('total_quarters', 0)} 분기")
            else:
                batch_results["batch_summary"]["failed_users"] += 1
                print(f"❌ User {user_id} 평가 실패: {result['error']}")
        
        batch_results["batch_metadata"]["end_time"] = datetime.now().isoformat()
        
        # 배치 결과 저장
        self._save_batch_results(batch_results)
        
        logger.info(f"배치 평가 완료 - 성공 사용자: {batch_results['batch_summary']['successful_users']}명, "
                   f"성공 분기: {batch_results['batch_summary']['successful_quarters']}개")
        return batch_results
    
    def _save_quarterly_results(self, results, user_id, employee_info, year, quarter, quarter_start, quarter_end):
        """분기별 결과 저장"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"evaluation_{user_id}_{year}Q{quarter}_{timestamp}.json"
        
        # 현재 날짜
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # 사용자 정보 추출
        user_name = employee_info.get('name', f"User_{user_id}")
        org_id = employee_info.get('organization_id', '')
        
        # 실제 조직명을 DB에서 조회
        if org_id:
            department = self.db_manager.get_organization_name(org_id)
        else:
            department = "미지정 팀"
        
        # 분기별 JSON 구조 생성
        final_results = {
            "type": "personal-quarter",
            "evaluated_year": int(year),
            "evaluated_quarter": int(quarter),
            "created_at": current_date,
            "title": f"{year} {quarter}분기 성과 리포트",
            "startDate": quarter_start,
            "endDate": quarter_end,
            "user": {
                "userId": int(user_id),
                "name": user_name,
                "department": department
            }
        }
        
        # 평가 결과 추가
        if "teamGoals" in results:
            final_results["teamGoals"] = results["teamGoals"]
        elif "error" not in results:
            final_results.update(results)
        else:
            final_results["error"] = results.get("error")
            final_results["error_details"] = results.get("error_details")
        
        # 참조 정보 추가
        final_results["reference"] = {
            "evaluation_timestamp": datetime.now().isoformat(),
            "user_id": user_id,
            "organization_info": {
                "organization_id": org_id,
                "organization_name": department
            },
            "quarter_info": {
                "year": year,
                "quarter": quarter,
                "total_weeks": employee_info.get('total_weeks', 0),
                "total_activities": employee_info.get('total_activities', 0)
            },
            "data_sources": {
                "weekly_data": f"Pinecone Index: {self.config.PINECONE_INDEX_NAME}",
                "team_data": f"MariaDB: {self.config.DB_CONFIG['host']}/{self.config.DB_CONFIG['database']}",
                "organization_data": "MariaDB.organizations"
            }
        }
        
        output_file = self.output_path / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"분기별 결과 저장 완료: {output_file} (팀: {department})")
        return str(output_file)
    
    def _save_batch_results(self, batch_results):
        """배치 결과 저장"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"batch_evaluation_{timestamp}.json"
        
        # 배치 결과에도 메타데이터 추가
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        final_batch_results = {
            "type": "batch-evaluation",
            "evaluated_year": 2024,
            "evaluated_quarter": 4,
            "created_at": current_date,
            "title": "2024 4분기 배치 성과 평가",
            "startDate": "2024-10-07",
            "endDate": "2024-12-27"
        }
        
        # 기존 배치 결과 추가
        final_batch_results.update(batch_results)
        
        output_file = self.output_path / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_batch_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"배치 결과 저장 완료: {output_file}")

# =====================================
# 메인 실행 함수
def main():
    """메인 실행 함수"""
    
    print("🎯 === 주간 보고서 분기별 배치 평가 시스템 ===")
    print("📋 Pinecone + MariaDB 기반 AI 평가 시스템")
    print("🔄 각 사용자별로 분기당 1개씩 JSON 파일 생성")
    
    try:
        print("\n🤖 시스템 초기화 중...")
        print("📋 환경변수 검증 중...")
        
        # Config 초기화 시 환경변수 검증이 자동으로 이루어짐
        evaluator = WeeklyReportEvaluator()
        print("✅ 시스템 초기화 완료!")
        
        while True:
            print("\n🎯 === 메인 메뉴 ===")
            print("1. 분기별 배치 평가 (모든 사용자)")
            print("2. 종료")
            
            choice = input("\n선택하세요 (1-2): ").strip()
            
            if choice == "1":
                # 사용 가능한 사용자 확인
                print("\n🔍 사용 가능한 사용자 조회 중...")
                available_users = evaluator.pinecone_manager.get_available_user_ids()
                
                if not available_users:
                    print("❌ 사용 가능한 사용자가 없습니다.")
                    continue
                
                print(f"\n📊 총 {len(available_users)}명의 사용자 분기별 배치 평가를 시작합니다.")
                print(f"📋 대상 사용자: {available_users}")
                print(f"⏱️ 예상 소요 시간: 약 {len(available_users) * 3}분")
                print(f"📁 각 사용자별로 분기당 1개씩 JSON 파일이 생성됩니다.")
                
                confirm = input("\n계속하시겠습니까? (y/N): ").strip().lower()
                
                if confirm not in ['y', 'yes']:
                    print("❌ 배치 평가를 취소합니다.")
                    continue
                
                print(f"\n🚀 분기별 배치 평가 시작...")
                print(f"💡 각 사용자의 데이터를 분기별로 분할하여 개별 평가합니다.")
                
                batch_result = evaluator.evaluate_batch_users()
                
                if "error" not in batch_result:
                    print(f"\n🎉 === 분기별 배치 평가 완료 ===")
                    print(f"📊 총 대상 사용자: {batch_result['batch_metadata']['total_users']}명")
                    print(f"✅ 성공한 사용자: {batch_result['batch_summary']['successful_users']}명")
                    print(f"❌ 실패한 사용자: {batch_result['batch_summary']['failed_users']}명")
                    
                    # 분기별 통계
                    print(f"\n📈 분기별 통계:")
                    print(f"   📊 총 처리된 분기: {batch_result['batch_summary']['total_quarters_processed']}개")
                    print(f"   ✅ 성공한 분기: {batch_result['batch_summary']['successful_quarters']}개")
                    print(f"   ❌ 실패한 분기: {batch_result['batch_summary']['failed_quarters']}개")
                    
                    # 성공률 계산
                    total_users = batch_result['batch_metadata']['total_users']
                    success_users = batch_result['batch_summary']['successful_users']
                    total_quarters = batch_result['batch_summary']['total_quarters_processed']
                    success_quarters = batch_result['batch_summary']['successful_quarters']
                    
                    if total_users > 0:
                        user_success_rate = (success_users / total_users * 100)
                        print(f"   📈 사용자 성공률: {user_success_rate:.1f}%")
                    
                    if total_quarters > 0:
                        quarter_success_rate = (success_quarters / total_quarters * 100)
                        print(f"   📈 분기 성공률: {quarter_success_rate:.1f}%")
                    
                    # 시간 정보
                    start_time = batch_result['batch_metadata']['start_time']
                    end_time = batch_result['batch_metadata']['end_time']
                    print(f"\n🕒 시작: {start_time}")
                    print(f"🕒 종료: {end_time}")
                    
                    # 실패한 사용자 상세 정보
                    failed_users = []
                    for user_id, result in batch_result['individual_results'].items():
                        if "error" in result:
                            failed_users.append(user_id)
                    
                    if failed_users:
                        print(f"\n❌ 실패한 사용자:")
                        for user_id in failed_users:
                            error_msg = batch_result['individual_results'][user_id].get('error', 'Unknown error')
                            print(f"   - User {user_id}: {error_msg}")
                    
                    # 성공한 사용자의 분기별 상세 정보
                    successful_details = []
                    for user_id, result in batch_result['individual_results'].items():
                        if "error" not in result and "quarterly_results" in result:
                            quarterly_info = result["quarterly_results"]
                            success_count = sum(1 for q in quarterly_info.values() if q["status"] == "success")
                            total_count = len(quarterly_info)
                            successful_details.append(f"User {user_id}: {success_count}/{total_count} 분기")
                    
                    if successful_details:
                        print(f"\n📋 사용자별 분기 성공 현황:")
                        for detail in successful_details:
                            print(f"   - {detail}")
                    
                    print(f"\n📁 결과 파일이 '{evaluator.config.OUTPUT_PATH}' 폴더에 저장되었습니다.")
                    print(f"💡 파일명 형식: evaluation_{{사용자ID}}_{{년도}}Q{{분기}}_{{타임스탬프}}.json")
                    print(f"📄 예시: evaluation_100_2024Q1_20250625_153054.json")
                    
                else:
                    print(f"❌ 배치 평가 실패: {batch_result['error']}")
            
            elif choice == "2":
                print("👋 시스템을 종료합니다.")
                break
            
            else:
                print("❌ 잘못된 선택입니다. 1-2 중에서 선택해주세요.")
        
    except ValueError as e:
        # 환경변수 관련 오류
        print(f"❌ 설정 오류: {e}")
        print("\n💡 해결 방법:")
        print("1. .env 파일이 존재하는지 확인")
        print("2. 필수 환경변수들이 올바르게 설정되었는지 확인")
        print("3. API 키들이 유효한지 확인")
        
    except KeyboardInterrupt:
        print("\n\n👋 사용자가 중단했습니다.")
    except Exception as e:
        logger.error(f"메인 실행 오류: {str(e)}")
        print(f"❌ 시스템 오류: {e}")
        
        # 에러 상세 정보 표시
        import traceback
        print(f"\n📋 에러 상세:")
        traceback.print_exc()


if __name__ == "__main__":
    main()