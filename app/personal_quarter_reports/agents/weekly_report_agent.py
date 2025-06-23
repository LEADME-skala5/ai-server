import json
import pandas as pd
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime
import logging
import openai
import os
import pymysql
from pymongo import MongoClient

from weekly_evaluations import (
    get_average_grade,
    get_weighted_workload_score,
    calculate_final_score
)

try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ .env 파일 로드 완료")
except ImportError:
    print("⚠️ python-dotenv 패키지가 설치되지 않음 - pip install python-dotenv")
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBManager:
    """MongoDB 연결 및 관리 클래스"""
    
    def __init__(self):
        self.mongodb_uri = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/"
        self.database_name = os.getenv("MONGO_DB_NAME")
        self.collection_name = "personal_quarter_reports"
        self.client = None
        
        print(f"📋 MongoDB 설정 로드 완료: {os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/{self.database_name}")
    
    def connect(self):
        """MongoDB 연결"""
        try:
            self.client = MongoClient(self.mongodb_uri)
            self.client.admin.command('ping')
            print("✅ MongoDB 연결 성공!")
            return True
        except Exception as e:
            print(f"❌ MongoDB 연결 실패: {e}")
            return False
    
    def add_user_to_quarter_document(self, user_data: Dict) -> bool:
        """분기별 문서에 사용자 데이터 추가"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db[self.collection_name]
            
            quarter_key = f"{user_data['year']}Q{user_data['quarter']}"
            
            # 해당 분기 문서가 존재하는지 확인
            existing_doc = collection.find_one({
                "quarter": quarter_key,
                "data_type": "weekly_evaluation_results"
            })
            
            if existing_doc:
                # 기존 문서에 사용자 데이터 추가
                collection.update_one(
                    {"quarter": quarter_key, "data_type": "weekly_evaluation_results"},
                    {
                        "$push": {"users": user_data},
                        "$set": {"updated_at": datetime.now()},
                        "$inc": {"user_count": 1}
                    }
                )
                print(f"✅ 기존 분기 문서에 사용자 ID {user_data['user_id']} 추가 완료")
            else:
                # 새로운 분기 문서 생성
                quarter_document = {
                    "quarter": quarter_key,
                    "year": user_data['year'],
                    "quarter_num": user_data['quarter'],
                    "data_type": "weekly_evaluation_results",
                    "user_count": 1,
                    "users": [user_data],
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
                
                result = collection.insert_one(quarter_document)
                print(f"✅ 새로운 분기 문서 생성 및 사용자 ID {user_data['user_id']} 추가 완료 - Document ID: {result.inserted_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 사용자 데이터 추가 실패 (사용자 ID: {user_data.get('user_id', 'unknown')}): {e}")
            return False
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()
            print("MongoDB 연결 종료")

class WeeklyReportAgent:
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(".env 파일에 OPENAI_API_KEY가 없습니다.")

        masked_key = api_key[:10] + "*" * (len(api_key) - 15) + api_key[-5:]
        print(f"🔑 OpenAI API 키 로드 완료: {masked_key}")
        self.client = openai.OpenAI(api_key=api_key)
        
        # MariaDB 연결 설정
        self.db_config = {
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT")),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "db": os.getenv("DB_NAME"),
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor,
            "autocommit": True
        }
        
        # MongoDB 매니저 초기화
        self.mongodb_manager = MongoDBManager()

    def update_weekly_score_in_db(self, user_id: int, evaluation_year: int, evaluation_quarter: int, weekly_score: float):
        """user_quarter_scores 테이블의 weekly_score 컬럼 업데이트"""
        conn = pymysql.connect(**self.db_config)
        try:
            with conn.cursor() as cur:
                # 기존 데이터 확인
                cur.execute(
                    """SELECT id FROM user_quarter_scores 
                       WHERE user_id = %s AND evaluation_year = %s AND evaluation_quarter = %s""",
                    (user_id, evaluation_year, evaluation_quarter)
                )
                existing_record = cur.fetchone()
                
                if existing_record:
                    # 기존 데이터 업데이트
                    cur.execute(
                        """UPDATE user_quarter_scores 
                           SET weekly_score = %s, updated_at = CURRENT_TIMESTAMP
                           WHERE user_id = %s AND evaluation_year = %s AND evaluation_quarter = %s""",
                        (weekly_score, user_id, evaluation_year, evaluation_quarter)
                    )
                    print(f"✅ user_quarter_scores weekly_score 업데이트 완료: 사용자 ID {user_id}, 점수 {weekly_score}")
                else:
                    # 새 데이터 추가
                    cur.execute(
                        """INSERT INTO user_quarter_scores (user_id, evaluation_year, evaluation_quarter, weekly_score)
                           VALUES (%s, %s, %s, %s)""",
                        (user_id, evaluation_year, evaluation_quarter, weekly_score)
                    )
                    print(f"✅ user_quarter_scores 새 데이터 추가 완료: 사용자 ID {user_id}, 점수 {weekly_score}")
        except Exception as e:
            print(f"❌ DB 업데이트 오류: {e}")
        finally:
            conn.close()

    def load_evaluation_data(self, input_path: str) -> Dict:
        with open(input_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def create_weekly_report_json(self, evaluation_data: Dict, evaluation_year: int, evaluation_quarter: int, save_to_mongodb: bool = True) -> Dict:
        emp_summary = evaluation_data['employee_summary']
        basic_info = emp_summary['basic_info']
        activity_categorization = emp_summary['activity_categorization']
        pattern_analysis = emp_summary.get('performance_pattern_analysis', {})

        # ✅ 정량 평가 점수 계산 (weeklyScore로 변경)
        employee_id = basic_info.get('employee_number')
        
        # employee_id가 'EMP001' 형태인 경우 숫자만 추출
        if isinstance(employee_id, str) and employee_id.startswith('EMP'):
            try:
                employee_id = int(employee_id[3:])  # 'EMP001' → 1
            except ValueError:
                logger.warning(f"employee_number 형식 오류: {employee_id}")
                employee_id = 0
        elif employee_id is not None:
            employee_id = int(employee_id)
        else:
            employee_id = 0
            
        # period에서 년도와 분기 추출, 실패 시 기존 방식 사용
        period = basic_info.get('period', '')
        year, quarter = self._extract_year_quarter_from_period(period)
        
        # 파싱 실패 시 매개변수 값 사용
        if year is None or quarter is None:
            year = evaluation_year
            quarter = evaluation_quarter

        try:
            print(f"🔍 점수 계산 시도: employee_id={employee_id}, year={year}, quarter={quarter}")
            avg_score = get_average_grade(employee_id, year, quarter)
            workload_score = get_weighted_workload_score(employee_id, year, quarter)
            weekly_score = calculate_final_score(avg_score, workload_score)
            
            # Decimal을 float로 변환
            avg_score = float(avg_score) if avg_score is not None else 0.0
            workload_score = float(workload_score) if workload_score is not None else 0.0
            weekly_score = float(weekly_score) if weekly_score is not None else 0.0
            
            print(f"📊 계산된 점수: avg={avg_score}, workload={workload_score}, weekly={weekly_score}")
            
            # ✅ DB에 weekly_score 업데이트
            self.update_weekly_score_in_db(employee_id, year, quarter, weekly_score)
            
        except Exception as e:
            logger.warning(f"정량 점수 계산 실패: {e}")
            avg_score = workload_score = weekly_score = 0.0

        employee_info = {
            "name": basic_info['name'],
            "department": basic_info.get('department', '클라우드 개발 3팀'),
            "period": basic_info['period']
        }

        all_team_goals = [
            "Cloud Professional 업무 진행 통한 BR/UR 개선",
            "CSP 파트너쉽 강화 통한 원가개선",
            "Cloud 마케팅 및 홍보 통한 대외 Cloud 고객확보",
            "글로벌 사업 Tech-presales 진행"
        ]

        team_goals_data = []
        key_achievements_data = []

        for goal in all_team_goals:
            matched = next((cat for cat in activity_categorization if cat['category'] == goal), None)
            if matched:
                count = matched.get('count', 0)
                activities = matched.get('activities', [])
                team_goals_data.append({
                    "goalName": goal,
                    "assigned": "배정" if count > 0 else "미배정",
                    "content": ", ".join(activities) if activities else "-",
                    "contributionCount": count
                })
                if count > 0:
                    key_achievements_data.append(f"{goal}: {count}건")
            else:
                team_goals_data.append({
                    "goalName": goal,
                    "assigned": "미배정",
                    "content": "-",
                    "contributionCount": 0
                })

        total_activities = basic_info['total_activities']
        active_goals = len([g for g in team_goals_data if g['contributionCount'] > 0])
        coverage = (active_goals / 4) * 100

        key_achievements_summary = [
            f"총 수행 활동: {total_activities}건 (목표 대비 평가)",
            f"목표 참여도: {active_goals}/4개 목표 참여 ({coverage:.0f}% 커버리지)"
        ]
        final_key_achievements = key_achievements_summary + key_achievements_data

        # ✅ AI 요약 생성
        quarterly_summary = self._generate_ai_quarterly_summary_text_only(
            basic_info, activity_categorization, pattern_analysis
        )

        # 결과 데이터 구성
        result_data = {
            "user_id": employee_id,
            "year": year,
            "quarter": quarter,
            "employee": employee_info,
            "teamGoals": team_goals_data,
            "keyAchievements": final_key_achievements,
            "quarterlyPerformanceSummary": quarterly_summary,
            "evaluationScore": {
                "averageScore": avg_score,
                "workloadScore": workload_score,
                "weeklyScore": weekly_score
            },
            "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 분기별 문서에 사용자 데이터 추가
        if save_to_mongodb:
            mongodb_save_success = self.mongodb_manager.add_user_to_quarter_document(result_data)
            
            if mongodb_save_success:
                print(f"✅ 사용자 ID {employee_id} 주간평가 분기별 문서에 추가 완료")
            else:
                print(f"❌ 사용자 ID {employee_id} 주간평가 MongoDB 저장 실패")
        
        result = {
            "success": True,
            "data": result_data
        }
        
        return result

    def process_single_quarter_weekly(self, input_files: List[Path], evaluation_year: int, evaluation_quarter: int):
        """단일 분기 주간 평가 처리 - 분기별 문서에 사용자 데이터 추가"""
        print(f"\n=== {evaluation_year}년 {evaluation_quarter}분기 주간 평가 처리 시작 ===")
        print(f"처리할 파일 수: {len(input_files)}개")
        print(f"MongoDB 저장 방식: {evaluation_year}Q{evaluation_quarter} 문서에 사용자 데이터 추가")
        print("=" * 50)
        
        results = []
        successful_count = 0
        failed_count = 0
        
        for i, file in enumerate(input_files, 1):
            # 진행률 표시 (매 10개 파일마다)
            if i % 10 == 0 or i == len(input_files):
                print(f"처리 진행률: {i}/{len(input_files)} ({i/len(input_files)*100:.1f}%)")
            
            try:
                data = self.load_evaluation_data(str(file))
                result = self.create_weekly_report_json(data, evaluation_year, evaluation_quarter, save_to_mongodb=True)
                results.append(result)
                successful_count += 1
                
                emp_id = result["data"]["user_id"]
                weekly_score = result["data"]["evaluationScore"]["weeklyScore"]
                print(f"✓ User {emp_id}: weekly_score={weekly_score:.2f} → 분기별 문서에 추가 완료")
                
            except Exception as e:
                print(f"❌ {file.name} 처리 실패: {e}")
                results.append({
                    "success": False,
                    "message": f"파일 처리 실패: {str(e)}",
                    "data": None
                })
                failed_count += 1
        
        # 통계 계산
        print(f"\n=== {evaluation_quarter}분기 주간 평가 처리 완료 ===")
        print(f"성공: {successful_count}명 → {evaluation_year}Q{evaluation_quarter} 문서에 추가 완료")
        print(f"실패: {failed_count}명")
        
        avg_score = None
        if successful_count > 0:
            scores = [r["data"]["evaluationScore"]["weeklyScore"] for r in results if r["success"]]
            if scores:
                avg_score = sum(scores) / len(scores)
                max_score = max(scores)
                min_score = min(scores)
                
                print(f"평균 점수: {avg_score:.2f}")
                print(f"최고 점수: {max_score:.2f}")
                print(f"최저 점수: {min_score:.2f}")
        
        # 실패한 파일 개수만 출력
        if failed_count > 0:
            print(f"처리 실패한 파일: {failed_count}개")
        
        return {
            "quarter": evaluation_quarter,
            "successful_count": successful_count,
            "failed_count": failed_count,
            "average_score": round(avg_score, 2) if avg_score else 0
        }

    def _generate_ai_quarterly_summary_text_only(self, basic_info: Dict, activity_categorization: List, pattern_analysis: Dict) -> str:
        prompt = f"""
다음 직원의 분기 성과 데이터를 분석하여 전문적인 성과요약 텍스트를 작성해주세요.

직원 정보:
- 이름: {basic_info['name']}
- 평가 기간: {basic_info['period']}
- 총 활동 수: {basic_info['total_activities']}건

활동 현황:
"""
        for cat in activity_categorization:
            prompt += f"- {cat['category']}: {cat['count']}건\n"
            if cat.get('activities'):
                prompt += f"  주요 활동: {', '.join(cat['activities'][:2])}\n"
            prompt += f"  기여도: {cat.get('impact', '중간')}\n"

        prompt += f"""
강점: {', '.join(pattern_analysis.get('strengths', []))}
개선점: {', '.join(pattern_analysis.get('improvements', []))}

요구사항:
- 전문적인 성과요약 작성 (200~300자)
- "{basic_info['name']} 직원은 {basic_info['period']} 기간 동안..." 으로 시작
"""
        try:
            res = self.client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {"role": "system", "content": "당신은 HR 전문가입니다."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=500
            )
            return res.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"AI 요약 실패: {e}")
            return f"{basic_info['name']} 직원은 {basic_info['period']} 기간 동안 총 {basic_info['total_activities']}건의 활동을 수행했습니다."

    def _extract_year_quarter_from_period(self, period: str) -> tuple:
        """period 문자열에서 년도와 분기를 추출
        예: "2024-01-07 ~ 2024-03-27" → (2024, 1)
        파싱 실패 시 (None, None) 반환
        """
        try:
            # period에서 시작 날짜 추출
            if ' ~ ' in period:
                start_date_str = period.split(' ~ ')[0].strip()
            else:
                start_date_str = period.strip()
            
            # 날짜 파싱 (YYYY-MM-DD 형식)
            if '-' in start_date_str and len(start_date_str) >= 7:
                year_str, month_str = start_date_str.split('-')[:2]
                year = int(year_str)
                month = int(month_str)
                
                # 월을 분기로 변환
                if 1 <= month <= 3:
                    quarter = 1
                elif 4 <= month <= 6:
                    quarter = 2
                elif 7 <= month <= 9:
                    quarter = 3
                elif 10 <= month <= 12:
                    quarter = 4
                else:
                    return None, None
                    
                return year, quarter
            else:
                return None, None
                
        except (ValueError, IndexError) as e:
            logger.warning(f"period 파싱 오류: {period}, {e}")
            return None, None

def main():
    print("🚀 주간 평가 보고서 생성 시작 (분기별 문서 저장 방식)")
    print("=" * 60)
    
    # 에이전트 초기화
    agent = WeeklyReportAgent()
    
    # MongoDB 연결 테스트
    print("🔌 MongoDB 연결 테스트...")
    if not agent.mongodb_manager.connect():
        print("❌ MongoDB 연결 실패. 로컬 저장만 진행합니다.")
    
    # 입력 파일 위치
    input_dir = Path("./output")
    output_dir = Path("./reports")
    output_dir.mkdir(exist_ok=True)
    
    # 입력 파일 검색
    files = list(input_dir.glob("evaluation_EMP*.json"))
    if not files:
        print("❌ ./output 디렉토리에 evaluation_EMP*.json 파일이 없습니다.")
        return
    
    print(f"발견된 파일 수: {len(files)}개")
    
    print(f"\n=== 2024년 전체 분기 주간 평가 배치 처리 시작 (분기별 문서 저장) ===")
    print(f"저장 방식: 분기별 문서에 사용자 데이터 누적 추가")
    print(f"저장 위치: MongoDB - {os.getenv('MONGO_DB_NAME')}.personal_quarter_reports")
    print(f"문서 구조:")
    print(f"  - 2024Q1 문서: Q1 모든 사용자 데이터")
    print(f"  - 2024Q2 문서: Q2 모든 사용자 데이터")
    print(f"  - 2024Q3 문서: Q3 모든 사용자 데이터")
    print(f"  - 2024Q4 문서: Q4 모든 사용자 데이터")
    print("=" * 60)
    
    # 전체 결과 저장용
    all_quarters_results = {}
    
    # 4개 분기 모두 처리
    for quarter in [1, 2, 3, 4]:
        quarter_result = agent.process_single_quarter_weekly(files, 2024, quarter)
        all_quarters_results[f"Q{quarter}"] = quarter_result
        
        # 로컬 파일도 저장 (백업용)
        backup_filename = f"weekly_evaluation_results_2024Q{quarter}_backup.json"
        with open(backup_filename, 'w', encoding='utf-8') as f:
            json.dump(quarter_result, f, ensure_ascii=False, indent=2)
        print(f"📄 백업 파일 저장 완료: {backup_filename}")
        
        # 분기 간 구분을 위한 여백
        print("\n" + "=" * 60)
    
    # 전체 분기 통합 결과 출력
    print(f"\n=== 2024년 전체 분기 주간 평가 처리 완료 ===")
    
    total_processed = 0
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"]
            total_processed += successful
            print(f"Q{quarter}: 성공 {successful}명 → 2024Q{quarter} 문서에 저장 완료")
        else:
            print(f"Q{quarter}: 데이터 없음")
    
    print(f"\n🎉 처리 완료 요약:")
    print(f"  - 총 처리된 사용자: {total_processed}명")
    print(f"  - 저장 방식: 분기별 하나의 문서에 모든 사용자 데이터 저장")
    print(f"  - 데이터베이스: {os.getenv('MONGO_DB_NAME')}")
    print(f"  - 컬렉션: personal_quarter_reports")
    print(f"  - 총 문서 수: 4개 (2024Q1, 2024Q2, 2024Q3, 2024Q4)")
    print(f"  - 문서 구조: quarter/year/quarter_num/data_type/user_count/users[]")
    print(f"  - MariaDB user_quarter_scores.weekly_score 업데이트 완료")
    
    # MongoDB 연결 종료
    agent.mongodb_manager.close()

if __name__ == "__main__":
    main()