import pandas as pd
import json
import openai
from typing import Dict, List, Any, Optional, Tuple
import os
from datetime import datetime
from pathlib import Path
import logging

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
                 api_key: Optional[str] = None, 
                 model: str = "gpt-4-turbo",
                 base_data_path: str = "./data",
                 output_path: str = "./output"):
        """
        AI 기반 주간 보고서 평가 에이전트
        
        Args:
            api_key: OpenAI API 키 (환경변수 OPENAI_API_KEY로도 설정 가능)
            model: 사용할 LLM 모델명
            base_data_path: 데이터 파일들이 위치한 기본 경로
            output_path: 결과 파일들을 저장할 경로
        """
        # API 키 설정 - 여러 방법으로 시도
        final_api_key = api_key or os.getenv("OPENAI_API_KEY")
        
        if not final_api_key:
            raise ValueError(
                "OpenAI API 키가 설정되지 않았습니다.\n"
                "다음 방법 중 하나를 사용하세요:\n"
                "1. 환경변수: set OPENAI_API_KEY=your-key (Windows)\n"
                "2. .env 파일: OPENAI_API_KEY=your-key\n"
                "3. 코드에서 직접 전달: WeeklyReportEvaluationAgent(api_key='your-key')"
            )
        
        self.client = openai.OpenAI(api_key=final_api_key)
        self.model = model
        self.base_data_path = Path(base_data_path)
        self.output_path = Path(output_path)
        
        # 데이터 저장소
        self.weekly_data = None
        self.team_criteria = None
        self.team_goals = None
        
        # 에이전트 상태 추적
        self.evaluation_history = []
        self.current_context = {}
        
        # 출력 디렉토리 생성
        self.output_path.mkdir(exist_ok=True)
        
        logger.info(f"WeeklyReportEvaluationAgent 초기화 완료 - 모델: {model}")
        
    def plan_evaluation(self, 
                       weekly_file: str = "weekly.csv",
                       criteria_file: str = "team_criteria.csv", 
                       goals_file: str = "team_goal.csv",
                       target_employees: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        평가 계획을 수립합니다.
        """
        logger.info("=== 평가 계획 수립 시작 ===")
        
        plan = {
            "timestamp": datetime.now().isoformat(),
            "data_files": {
                "weekly": weekly_file,
                "criteria": criteria_file,
                "goals": goals_file
            },
            "target_employees": target_employees,
            "steps": []
        }
        
        # 1단계: 데이터 검증
        plan["steps"].append({
            "step": 1,
            "action": "데이터 파일 존재 여부 및 구조 검증",
            "status": "planned"
        })
        
        # 2단계: 데이터 로드 및 전처리
        plan["steps"].append({
            "step": 2,
            "action": "CSV 파일 로드 및 데이터 무결성 검사",
            "status": "planned"
        })
        
        # 3단계: 평가 대상 분석
        plan["steps"].append({
            "step": 3,
            "action": "평가 대상 직원 및 팀 구조 분석",
            "status": "planned"
        })
        
        # 4단계: 평가 실행
        plan["steps"].append({
            "step": 4,
            "action": "개별 직원 평가 수행",
            "status": "planned"
        })
        
        # 5단계: 결과 검증 및 저장
        plan["steps"].append({
            "step": 5,
            "action": "평가 결과 검증 및 파일 저장",
            "status": "planned"
        })
        
        self.current_context["plan"] = plan
        logger.info(f"평가 계획 수립 완료 - {len(plan['steps'])}단계")
        
        return plan
    
    def validate_data_files(self, 
                           weekly_file: str,
                           criteria_file: str, 
                           goals_file: str) -> Dict[str, Any]:
        """
        데이터 파일들의 존재 여부와 구조를 검증합니다.
        """
        logger.info("데이터 파일 검증 시작")
        
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "file_info": {}
        }
        
        files_to_check = {
            "weekly": weekly_file,
            "criteria": criteria_file,
            "goals": goals_file
        }
        
        for file_type, filename in files_to_check.items():
            file_path = self.base_data_path / filename
            
            if not file_path.exists():
                validation_result["errors"].append(f"{file_type} 파일이 존재하지 않습니다: {file_path}")
                validation_result["valid"] = False
                continue
                
            try:
                # 파일 기본 정보 수집
                df = pd.read_csv(file_path)
                validation_result["file_info"][file_type] = {
                    "path": str(file_path),
                    "rows": len(df),
                    "columns": list(df.columns),
                    "size_mb": file_path.stat().st_size / (1024 * 1024)
                }
                
                # 필수 컬럼 체크
                if file_type == "weekly":
                    required_cols = ["employee_number", "done_task"]
                    missing_cols = [col for col in required_cols if col not in df.columns]
                    if missing_cols:
                        validation_result["errors"].append(
                            f"weekly 파일에 필수 컬럼이 없습니다: {missing_cols}"
                        )
                        validation_result["valid"] = False
                        
            except Exception as e:
                validation_result["errors"].append(f"{file_type} 파일 읽기 오류: {str(e)}")
                validation_result["valid"] = False
        
        if validation_result["valid"]:
            logger.info("모든 데이터 파일 검증 성공")
        else:
            logger.error(f"데이터 파일 검증 실패: {validation_result['errors']}")
            
        return validation_result
    
    def load_and_preprocess_data(self, 
                                weekly_file: str,
                                criteria_file: str, 
                                goals_file: str) -> Dict[str, Any]:
        """
        데이터를 로드하고 전처리합니다.
        """
        logger.info("데이터 로드 및 전처리 시작")
        
        try:
            self.weekly_data = pd.read_csv(self.base_data_path / weekly_file)
            self.team_criteria = pd.read_csv(self.base_data_path / criteria_file)
            self.team_goals = pd.read_csv(self.base_data_path / goals_file)
            
            # 데이터 전처리
            preprocessing_result = {
                "weekly_records": len(self.weekly_data),
                "unique_employees": self.weekly_data['employee_number'].nunique() if 'employee_number' in self.weekly_data.columns else 0,
                "teams_in_goals": self._extract_teams_from_data(),
                "date_range": self._extract_date_range(),
                "data_quality": self._assess_data_quality()
            }
            
            logger.info(f"데이터 로드 완료 - 직원 {preprocessing_result['unique_employees']}명, 기록 {preprocessing_result['weekly_records']}건")
            return preprocessing_result
            
        except Exception as e:
            logger.error(f"데이터 로드 실패: {str(e)}")
            raise ValueError(f"데이터 로드 중 오류 발생: {str(e)}")
    
    def _extract_teams_from_data(self) -> List[str]:
        """데이터에서 팀 목록을 추출합니다."""
        teams = set()
        
        # weekly 데이터에서 팀 추출
        if self.weekly_data is not None:
            team_columns = ['team', 'organization', '조직', '팀']
            for col in team_columns:
                if col in self.weekly_data.columns:
                    teams.update(self.weekly_data[col].dropna().unique())
                    break
        
        # goals 데이터에서 팀 추출
        if self.team_goals is not None:
            team_column = self._find_column_by_keywords(
                self.team_goals, 
                ['team', 'org', 'group', 'dept', '팀', '조직', '부서']
            )
            if team_column:
                teams.update(self.team_goals[team_column].dropna().unique())
        
        return sorted(list(teams))
    
    def _extract_date_range(self) -> Dict[str, str]:
        """데이터의 날짜 범위를 추출합니다."""
        if self.weekly_data is None:
            return {}
            
        date_info = {}
        for date_col in ['start_date', 'finish_date', 'date']:
            if date_col in self.weekly_data.columns:
                dates = pd.to_datetime(self.weekly_data[date_col], errors='coerce').dropna()
                if not dates.empty:
                    date_info[f"{date_col}_min"] = dates.min().strftime('%Y-%m-%d')
                    date_info[f"{date_col}_max"] = dates.max().strftime('%Y-%m-%d')
        
        return date_info
    
    def _assess_data_quality(self) -> Dict[str, Any]:
        """데이터 품질을 평가합니다."""
        quality_report = {}
        
        if self.weekly_data is not None:
            quality_report["weekly"] = {
                "missing_employee_numbers": self.weekly_data['employee_number'].isnull().sum() if 'employee_number' in self.weekly_data.columns else 0,
                "missing_tasks": self.weekly_data['done_task'].isnull().sum() if 'done_task' in self.weekly_data.columns else 0,
                "duplicate_records": self.weekly_data.duplicated().sum(),
                "empty_task_content": (self.weekly_data['done_task'].str.strip() == '').sum() if 'done_task' in self.weekly_data.columns else 0
            }
        
        return quality_report
    
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
            "weekly_tasks": employee_data[['start_date', 'finish_date', 'done_task']].to_dict('records') if all(col in employee_data.columns for col in ['start_date', 'finish_date', 'done_task']) else []
        }
        
        logger.info(f"직원 {employee_number} 데이터 분석 완료")
        return context
    
    def _extract_employee_info(self, employee_data: pd.DataFrame) -> Dict[str, Any]:
        """직원 기본 정보를 추출합니다."""
        info = {
            "name": employee_data['name'].iloc[0] if 'name' in employee_data.columns else "Unknown",
            "employee_number": employee_data['employee_number'].iloc[0],
            "department": employee_data['department'].iloc[0] if 'department' in employee_data.columns else "",
            "team": employee_data['team'].iloc[0] if 'team' in employee_data.columns else "",
            "period": "",
            "total_weeks": len(employee_data)
        }
        
        # 날짜 범위 설정
        if 'start_date' in employee_data.columns and 'finish_date' in employee_data.columns:
            start_dates = pd.to_datetime(employee_data['start_date'], errors='coerce').dropna()
            finish_dates = pd.to_datetime(employee_data['finish_date'], errors='coerce').dropna()
            if not start_dates.empty and not finish_dates.empty:
                info["period"] = f"{start_dates.min().strftime('%Y-%m-%d')} ~ {finish_dates.max().strftime('%Y-%m-%d')}"
        
        logger.info(f"직원 정보: {info['name']} ({info['department']} - {info['team']})")
        return info
    
    def _get_filtered_team_goals(self, employee_data: pd.DataFrame) -> List[Dict]:
        """해당 직원의 팀 목표만 필터링하여 반환합니다."""
        if self.team_goals is None or self.team_goals.empty:
            return []
            
        employee_team = employee_data['team'].iloc[0] if 'team' in employee_data.columns else ""
        team_column = self._find_column_by_keywords(
            self.team_goals, 
            ['team', 'org', 'group', 'dept', '팀', '조직', '부서']
        )
        
        if team_column and employee_team:
            filtered_goals = self.team_goals[
                self.team_goals[team_column] == employee_team
            ].to_dict('records')
            logger.info(f"팀 목표 필터링 완료: {len(filtered_goals)}개 목표")
            return filtered_goals
        else:
            logger.warning("팀 컬럼을 찾을 수 없어 전체 목표 반환")
            return self.team_goals.to_dict('records')
    
    def _get_filtered_team_criteria(self, employee_data: pd.DataFrame) -> List[Dict]:
        """해당 직원의 팀 평가 기준만 필터링하여 반환합니다."""
        if self.team_criteria is None or self.team_criteria.empty:
            return []
            
        employee_team = employee_data['team'].iloc[0] if 'team' in employee_data.columns else ""
        team_column = self._find_column_by_keywords(
            self.team_criteria, 
            ['team', 'org', 'group', 'dept', '팀', '조직', '부서']
        )
        
        if team_column and employee_team:
            filtered_criteria = self.team_criteria[
                self.team_criteria[team_column] == employee_team
            ].to_dict('records')
            logger.info(f"팀 기준 필터링 완료: {len(filtered_criteria)}개 기준")
            return filtered_criteria
        else:
            logger.warning("팀 컬럼을 찾을 수 없어 전체 기준 반환")
            return self.team_criteria.to_dict('records')
    
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
            team_categories = ["일반업무", "프로젝트관리", "고객대응", "기타활동"]
            logger.warning(f"팀 목표 카테고리를 추출할 수 없어 기본 카테고리 사용: {team_categories}")
        
        prompt = f"""
당신은 전문적인 HR 평가 에이전트입니다. 직원의 주간 보고서를 종합 분석하여 객관적인 성과 평가를 수행해주세요.

## 평가 대상 정보

### 직원 기본 정보
- 이름: {employee_data['employee_info']['name']}
- 직원번호: {employee_data['employee_info']['employee_number']}
- 소속: {employee_data['employee_info']['department']} - {employee_data['employee_info']['team']}
- 평가 기간: {employee_data['employee_info']['period']}
- 총 평가 주차: {employee_data['employee_info']['total_weeks']}주

### 주간별 수행 업무
"""
        
        # 주간별 업무 추가
        if employee_data['weekly_tasks']:
            for i, task in enumerate(employee_data['weekly_tasks'], 1):
                start_date = task.get('start_date', 'N/A')
                finish_date = task.get('finish_date', 'N/A')
                done_task = task.get('done_task', 'N/A')
                prompt += f"\n**{i}주차 ({start_date} ~ {finish_date})**\n"
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
        
        # 카테고리 가이드
        prompt += f"""
### 활동 분류 카테고리
다음 카테고리로 업무를 분류해주세요:
"""
        for i, category in enumerate(team_categories, 1):
            prompt += f"{i}. {category}\n"
        
        prompt += f"""

## 평가 결과 형식

다음 JSON 형식으로 종합 평가를 제공해주세요:

```json
{{
  "employee_summary": {{
    "basic_info": {{
      "name": "직원명",
      "employee_number": "직원번호",
      "period": "평가기간",
      "total_activities": "총 활동 수"
    }},
    "activity_categorization": [
"""
        
        # 카테고리별 구조 동적 생성 (배열 형태로 변경)
        for i, category in enumerate(team_categories):
            prompt += f"""      {{
        "category": "{category}",
        "count": 0,
        "activities": [],
        "impact": "목표 달성에 미친 영향 설명",
        "evidence": [],
        "assessment": "활동과 목표 간 연관성 평가"
      }}"""
            if i < len(team_categories) - 1:
                prompt += ","
            prompt += "\n"
        
        prompt += f"""    ],
    "performance_pattern_analysis": {{
      "strengths": ["강점1", "강점2", "강점3"],
      "improvements": ["개선점1", "개선점2"],
      "work_style": "업무 스타일 특징 요약"
    }}
  }}
}}
```

## 평가 가이드라인

1. **객관적 분석**: 구체적 수치와 성과를 중심으로 평가
2. **카테고리 매핑**: 각 업무를 적절한 카테고리로 분류
3. **활동 내용 기록**: activities 배열에는 반드시 실제 수행한 업무 내용을 기록하세요 (주차 정보가 아닌 구체적인 업무 설명)
4. **패턴 인식**: 반복되는 성공 요인과 개선 영역 식별
5. **균형적 평가**: 강점과 개선점을 균형있게 제시
6. **실행 가능성**: 구체적이고 실행 가능한 개선 방향 제시

## 중요사항
- activities 배열에는 "1주차", "2주차" 같은 주차 정보가 아닌, 해당 카테고리에서 실제로 수행한 구체적인 업무 내용을 기록해주세요.
- 예시: ["고객 문의 응답 및 이슈 해결", "신규 프로젝트 기획안 작성", "팀 미팅 진행 및 업무 분배"]

JSON 형식을 정확히 준수하여 응답해주세요.
"""
        
        return prompt
    
    def execute_llm_evaluation(self, prompt: str) -> Dict[str, Any]:
        """LLM을 사용하여 평가를 실행합니다."""
        try:
            logger.info(f"LLM 평가 실행 - 모델: {self.model}")
            print(f"🤖 OpenAI API 호출 시작... (모델: {self.model})")
            
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
                               filename: Optional[str] = None) -> str:
        """평가 결과를 저장합니다."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"evaluation_result_{timestamp}.json"
        
        output_file = self.output_path / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"평가 결과 저장 완료: {output_file}")
        return str(output_file)
    
    def execute_single_evaluation(self, 
                                 employee_number: str,
                                 weekly_file: str = "weekly.csv",
                                 criteria_file: str = "team_criteria.csv",
                                 goals_file: str = "team_goal.csv") -> Dict[str, Any]:
        """단일 직원에 대한 완전한 평가를 실행합니다."""
        
        logger.info(f"=== 직원 {employee_number} 평가 시작 ===")
        
        try:
            # 1단계: 계획 수립
            plan = self.plan_evaluation(weekly_file, criteria_file, goals_file, [employee_number])
            
            # 2단계: 데이터 검증
            validation = self.validate_data_files(weekly_file, criteria_file, goals_file)
            if not validation["valid"]:
                raise ValueError(f"데이터 검증 실패: {validation['errors']}")
            
            # 3단계: 데이터 로드
            self.load_and_preprocess_data(weekly_file, criteria_file, goals_file)
            
            # 4단계: 직원 데이터 분석
            employee_data = self.analyze_employee_data(employee_number)
            
            # 5단계: 프롬프트 생성
            prompt = self.generate_evaluation_prompt(employee_data)
            
            # 6단계: LLM 평가 실행
            evaluation_result = self.execute_llm_evaluation(prompt)
            
            # 7단계: 결과 저장 (AI 평가 결과만)
            if "error" not in evaluation_result:
                output_file = self.save_evaluation_results(
                    evaluation_result,  # AI 평가 결과만 저장
                    f"evaluation_{employee_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                
                # 평가 이력에 추가
                self.evaluation_history.append({
                    "employee_number": employee_number,
                    "timestamp": datetime.now().isoformat(),
                    "output_file": output_file,
                    "status": "success"
                })
                
                logger.info(f"직원 {employee_number} 평가 완료 - 성공")
                return evaluation_result
            else:
                # 오류 발생 시에도 간단한 결과 저장
                error_result = {
                    "employee_number": employee_number,
                    "timestamp": datetime.now().isoformat(),
                    "error": evaluation_result["error"]
                }
                
                output_file = self.save_evaluation_results(
                    error_result,
                    f"evaluation_error_{employee_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                
                self.evaluation_history.append({
                    "employee_number": employee_number,
                    "timestamp": datetime.now().isoformat(),
                    "output_file": output_file,
                    "status": "failed"
                })
                
                logger.info(f"직원 {employee_number} 평가 실패")
                return error_result
                
        except Exception as e:
            logger.error(f"직원 {employee_number} 평가 실패: {str(e)}")
            error_result = {
                "employee_number": employee_number,
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
            return error_result

    def execute_batch_evaluation(self, 
                                target_employees: Optional[List[str]] = None,
                                weekly_file: str = "weekly.csv",
                                criteria_file: str = "team_criteria.csv",
                                goals_file: str = "team_goal.csv") -> Dict[str, Any]:
        """다수 직원에 대한 배치 평가를 실행합니다."""
        
        logger.info("=== 배치 평가 시작 ===")
        
        try:
            # 데이터 로드
            self.load_and_preprocess_data(weekly_file, criteria_file, goals_file)
            
            # 대상 직원 목록 결정
            if target_employees is None:
                if 'employee_number' in self.weekly_data.columns:
                    target_employees = self.weekly_data['employee_number'].unique().tolist()
                else:
                    raise ValueError("employee_number 컬럼을 찾을 수 없습니다.")
            
            batch_results = {
                "batch_metadata": {
                    "start_time": datetime.now().isoformat(),
                    "target_employees": target_employees,
                    "total_employees": len(target_employees)
                },
                "individual_results": {},
                "batch_summary": {
                    "successful_evaluations": 0,
                    "failed_evaluations": 0
                }
            }
            
            # 개별 직원 평가 실행
            for employee_number in target_employees:
                logger.info(f"배치 평가 진행 중: {employee_number}")
                
                try:
                    result = self.execute_single_evaluation(
                        employee_number, weekly_file, criteria_file, goals_file
                    )
                    
                    batch_results["individual_results"][employee_number] = result
                    
                    if "error" not in result:
                        batch_results["batch_summary"]["successful_evaluations"] += 1
                    else:
                        batch_results["batch_summary"]["failed_evaluations"] += 1
                        
                except Exception as e:
                    logger.error(f"직원 {employee_number} 배치 평가 실패: {str(e)}")
                    batch_results["individual_results"][employee_number] = {
                        "error": str(e),
                        "employee_number": employee_number
                    }
                    batch_results["batch_summary"]["failed_evaluations"] += 1
            
            batch_results["batch_metadata"]["end_time"] = datetime.now().isoformat()
            
            # 배치 결과 저장
            batch_output_file = self.save_evaluation_results(
                batch_results,
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
                    "error": str(e)
                },
                "batch_summary": {
                    "successful_evaluations": 0,
                    "failed_evaluations": len(target_employees) if target_employees else 0
                }
            }

    def get_evaluation_history(self) -> List[Dict[str, Any]]:
        """평가 이력을 반환합니다."""
        return self.evaluation_history

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
            "evaluated_employees": [entry["employee_number"] for entry in self.evaluation_history]
        }
        
        return stats


# 사용 예시 및 테스트 함수
def main():
    """메인 실행 함수 - 모든 직원 배치 평가"""
    
    print("=== WeeklyReportEvaluationAgent 시작 ===")
    print("📋 모든 직원에 대한 배치 평가를 수행합니다.")
    
    # API 키 여러 방법으로 로드 시도
    api_key = None
    
     # API 키 확인 코드 추가
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        masked_key = api_key[:10] + "*" * (len(api_key) - 15) + api_key[-5:] if len(api_key) > 15 else api_key[:5] + "*" * (len(api_key) - 5)
        print(f"🔑 현재 API 키: {masked_key}")
        print(f"📏 API 키 길이: {len(api_key)} 문자")
        print(f"🏷️  API 키 형식: {'✅ 올바름' if api_key.startswith(('sk-', 'sk-proj-')) else '❌ 잘못됨'}")
    else:
        print("❌ API 키를 찾을 수 없습니다")
    
    # 1. 환경변수에서 확인
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        print("✓ 환경변수에서 API 키 발견")
    else:
        # 2. .env 파일에서 로드 시도
        env_file = Path(".env")
        if env_file.exists():
            print("✓ .env 파일 발견, 로딩 중...")
            try:
                with open(env_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith('OPENAI_API_KEY='):
                            api_key = line.split('=', 1)[1].strip()
                            os.environ['OPENAI_API_KEY'] = api_key
                            print("✓ .env 파일에서 API 키 로드 완료")
                            break
            except Exception as e:
                print(f"✗ .env 파일 읽기 실패: {e}")
        else:
            print("✗ .env 파일을 찾을 수 없음")
    
    # 3. API 키가 여전히 없으면 사용자 입력 요청
    if not api_key:
        print("\n📋 OpenAI API 키가 설정되지 않았습니다.")
        print("다음 중 하나의 방법을 선택하세요:")
        print("1. 직접 입력")
        print("2. 종료 후 환경변수 설정")
        
        choice = input("\n선택 (1 또는 2): ").strip()
        if choice == '1':
            api_key = input("OpenAI API 키를 입력하세요: ").strip()
            if not api_key:
                print("❌ API 키가 입력되지 않았습니다. 종료합니다.")
                return
        else:
            print("❌ 환경변수 설정 후 다시 실행해주세요.")
            print("Windows: set OPENAI_API_KEY=your-key")
            print("Linux/Mac: export OPENAI_API_KEY=your-key")
            return
    
    try:
        print(f"\n🤖 에이전트 초기화 중... (모델: gpt-4-turbo)")
        
        # 에이전트 초기화
        agent = WeeklyReportEvaluationAgent(
            api_key=api_key,
            model="gpt-4-turbo",
            base_data_path="../data",  # 상위 폴더의 data 디렉토리
            output_path="./output"
        )
        
        print("✅ 에이전트 초기화 완료!")
        
        # 데이터 로드해서 직원 수 미리 확인
        print("\n📊 데이터 분석 중...")
        agent.load_and_preprocess_data(
            weekly_file="weekly.csv",
            criteria_file="team_criteria.csv", 
            goals_file="team_goal.csv"
        )
        
        # 평가 대상 직원 목록 확인
        if 'employee_number' in agent.weekly_data.columns:
            target_employees = agent.weekly_data['employee_number'].unique().tolist()
            print(f"📋 평가 대상 직원: {len(target_employees)}명")
            print(f"   직원 목록: {target_employees}")
            
            # 사용자 확인
            proceed = input(f"\n{len(target_employees)}명의 직원을 평가하시겠습니까? (y/N): ").strip().lower()
            if proceed not in ['y', 'yes']:
                print("❌ 평가를 취소합니다.")
                return
        else:
            print("❌ employee_number 컬럼을 찾을 수 없습니다.")
            return
        
        # 배치 평가 실행
        print(f"\n🚀 {len(target_employees)}명 배치 평가 시작...")
        print("⏰ 이 작업은 시간이 오래 걸릴 수 있습니다...")
        
        batch_result = agent.execute_batch_evaluation(
            target_employees=None,  # None이면 모든 직원 자동 선택
            weekly_file="weekly.csv",
            criteria_file="team_criteria.csv", 
            goals_file="team_goal.csv"
        )
        
        # 배치 평가 결과 요약
        print(f"\n🎯 === 배치 평가 완료 ===")
        print(f"📊 총 대상 직원: {batch_result['batch_metadata']['total_employees']}명")
        print(f"✅ 성공한 평가: {batch_result['batch_summary']['successful_evaluations']}건")
        print(f"❌ 실패한 평가: {batch_result['batch_summary']['failed_evaluations']}건")
        
        # 개별 결과 간단 요약
        print(f"\n📋 개별 평가 결과:")
        for emp_num, result in batch_result['individual_results'].items():
            if "error" in result:
                print(f"   {emp_num}: ❌ 실패 - {result['error'][:50]}...")
            else:
                emp_name = result.get('employee_summary', {}).get('basic_info', {}).get('name', 'Unknown')
                print(f"   {emp_num} ({emp_name}): ✅ 성공")
        
        # 최종 통계
        stats = agent.get_evaluation_statistics()
        print(f"\n📈 === 최종 통계 ===")
        print(f"총 평가 수행: {stats['total_evaluations']}건")
        print(f"성공률: {(stats['successful_evaluations']/stats['total_evaluations']*100):.1f}%" if stats['total_evaluations'] > 0 else "N/A")
        
        # 결과 파일 위치 안내
        print(f"\n💾 결과 저장 위치:")
        print(f"   - 개별 평가 파일: ./output/evaluation_[직원번호]_[타임스탬프].json")
        print(f"   - 배치 결과 파일: ./output/batch_evaluation_[타임스탬프].json")
        
    except Exception as e:
        logger.error(f"메인 실행 오류: {str(e)}")
        print(f"❌ 오류 발생: {str(e)}")
        raise


if __name__ == "__main__":
    main()