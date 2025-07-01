import os
import pymysql
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv
from pathlib import Path
from pymongo import MongoClient

# 환경변수 로드
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# DB 설정
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True
}

# MongoDB 설정
MONGO_CONFIG = {
    "host": os.getenv("MONGO_HOST"),
    "port": int(os.getenv("MONGO_PORT")),
    "username": os.getenv("MONGO_USER"),
    "password": os.getenv("MONGO_PASSWORD"),
    "db_name": os.getenv("MONGO_DB_NAME")
}

class ComprehensiveReportGenerator:
    """종합 성과 리포트 생성기"""
    
    def __init__(self):
        self.mongodb_uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        self.database_name = MONGO_CONFIG["db_name"]
        self.client = None
        
        print(f"📋 MongoDB 설정 로드 완료: {MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{self.database_name}")
    
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
    
    def get_user_info(self, user_id: int) -> Dict:
        """MariaDB에서 사용자 기본 정보 조회"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT u.name, u.organization_id, j.name as job_name, u.job_years
                    FROM users u
                    LEFT JOIN jobs j ON u.job_id = j.id
                    WHERE u.id = %s
                """, (user_id,))
                result = cur.fetchone()
                
                if result:
                    return {
                        "name": result['name'],
                        "job_name": result['job_name'] or "미지정",
                        "job_years": result['job_years'] or 0,
                        "organization_id": result['organization_id']
                    }
        except Exception as e:
            print(f"❌ 사용자 정보 조회 실패 (user_id: {user_id}): {e}")
        finally:
            if 'conn' in locals():
                conn.close()
        
        return {
            "name": f"직원 {user_id}번",
            "job_name": "미지정", 
            "job_years": 0,
            "organization_id": None
        }
    
    def get_final_score(self, user_id: int, year: int, quarter: int) -> float:
        """MariaDB에서 최종 점수 조회"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT final_score 
                    FROM user_quarter_scores 
                    WHERE user_id = %s AND evaluation_year = %s AND evaluation_quarter = %s
                """, (user_id, year, quarter))
                result = cur.fetchone()
                return float(result['final_score']) if result and result['final_score'] else 0.0
        except Exception as e:
            print(f"❌ 최종 점수 조회 실패: {e}")
            return 0.0
        finally:
            if 'conn' in locals():
                conn.close()
    
    def get_data_from_collection(self, collection_name: str, user_id: int, year: int, quarter: int) -> Optional[Dict]:
        """특정 컬렉션에서 사용자 데이터 조회 (기존 컬렉션용)"""
        try:
            if not self.client:
                if not self.connect():
                    return None
            
            db = self.client[self.database_name]
            collection = db[collection_name]
            
            # type: "personal-quarter"로 문서 조회
            document = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": year,
                "evaluated_quarter": quarter
            })
            
            if not document or "users" not in document:
                return None
            
            # 해당 사용자 데이터 찾기
            for user_data in document["users"]:
                if user_data.get("user_id") == user_id:
                    return user_data
            
            return None
            
        except Exception as e:
            print(f"❌ {collection_name} 데이터 조회 실패 (user: {user_id}): {e}")
            return None
    
    def get_weekly_evaluation_data(self, user_id: int, year: int, quarter: int) -> Optional[Dict]:
        """weekly_evaluation_results에서 사용자별 분기 데이터 조회"""
        try:
            if not self.client:
                if not self.connect():
                    return None
            
            db = self.client[self.database_name]
            collection = db["weekly_evaluation_results"]
            
            # data_type: "personal-quarter"로 문서 조회
            document = collection.find_one({
                "data_type": "personal-quarter"
            })
            
            if not document or "users" not in document:
                print(f"❌ weekly_evaluation_results 문서 구조 오류")
                return None
            
            # 사용자 ID를 문자열로 변환하여 검색
            user_id_str = str(user_id)
            
            # users 객체에서 해당 사용자 찾기
            if user_id_str not in document["users"]:
                print(f"❌ 사용자 {user_id} 데이터가 weekly_evaluation_results에 없음")
                return None
            
            user_data = document["users"][user_id_str]
            
            # 해당 분기 데이터 추출
            quarter_key = f"{year}Q{quarter}"
            if "quarters" not in user_data or quarter_key not in user_data["quarters"]:
                print(f"❌ 사용자 {user_id}의 {quarter_key} 데이터가 없음")
                return None
            
            quarter_data = user_data["quarters"][quarter_key]
            
            print(f"✅ 사용자 {user_id}의 {quarter_key} weekly 데이터 조회 성공")
            return quarter_data
            
        except Exception as e:
            print(f"❌ weekly_evaluation_results 데이터 조회 실패 (user: {user_id}, {year}Q{quarter}): {e}")
            return None
    
    def calculate_percentile_text(self, rank: int, total: int) -> str:
        """랭킹을 퍼센타일 텍스트로 변환"""
        if total == 0:
            return "데이터 없음"
        
        percentile = (rank / total) * 100
        
        if percentile <= 10:
            return "상위 10%"
        elif percentile <= 20:
            return "상위 20%"
        elif percentile <= 30:
            return "상위 30%"
        elif percentile <= 40:
            return "상위 40%"
        elif percentile <= 50:
            return "상위 50%"
        else:
            return f"상위 {int(percentile)}%"
    
    def generate_quarter_dates(self, year: int, quarter: int) -> tuple:
        """분기별 시작일과 종료일 계산"""
        if quarter == 1:
            start_date = f"{year}-01-01"
            end_date = f"{year}-03-31"
        elif quarter == 2:
            start_date = f"{year}-04-01"
            end_date = f"{year}-06-30"
        elif quarter == 3:
            start_date = f"{year}-07-01"
            end_date = f"{year}-09-30"
        else:  # quarter == 4
            start_date = f"{year}-10-01"
            end_date = f"{year}-12-31"
        
        return start_date, end_date
    
    def calculate_key_achievements_from_goals(self, team_goals: List[Dict]) -> List[str]:
        """팀 목표에서 주요 성과 통계 계산"""
        if not team_goals:
            return ["활동 데이터 없음"]
        
        total_activities = sum(goal.get("contributionCount", 0) for goal in team_goals)
        assigned_goals = sum(1 for goal in team_goals if goal.get("assigned") == "배정" and goal.get("contributionCount", 0) > 0)
        total_goals = len(team_goals)
        coverage = (assigned_goals / total_goals * 100) if total_goals > 0 else 0
        
        achievements = [
            f"총 수행 활동: {total_activities}건 (목표 대비 평가)",
            f"목표 참여도: {assigned_goals}/{total_goals}개 목표 참여 ({coverage:.0f}% 커버리지)"
        ]
        
        # 목표별 활동 건수 추가
        for goal in team_goals:
            contribution_count = goal.get("contributionCount", 0)
            if contribution_count > 0:
                achievements.append(f"{goal['goalName']}: {contribution_count}건")
        
        return achievements
    
    def generate_comprehensive_report(self, user_id: int, year: int, quarter: int) -> Dict:
        """종합 성과 리포트 생성 (개선 버전)"""
        print(f"🎯 사용자 ID {user_id}의 {year}Q{quarter} 종합 리포트 생성 중...")
        
        # 1. 기본 사용자 정보 조회
        user_info = self.get_user_info(user_id)
        
        # 2. weekly_evaluation_results에서 상세 정보 추출
        weekly_quarter_data = self.get_weekly_evaluation_data(user_id, year, quarter)
        
        # 3. 기본 분기 날짜 계산
        start_date, end_date = self.generate_quarter_dates(year, quarter)
        
        # 4. weekly 데이터에서 실제 날짜가 있다면 사용
        if weekly_quarter_data:
            start_date = weekly_quarter_data.get("startDate", start_date)
            end_date = weekly_quarter_data.get("endDate", end_date)
        
        # 5. 부서명 추출
        department = ""
        if weekly_quarter_data and "user" in weekly_quarter_data:
            department = weekly_quarter_data["user"].get("department", "")
        
        # 6. 팀 목표 및 주요 성과 추출
        team_goals = []
        key_achievements = []
        
        if weekly_quarter_data and "teamGoals" in weekly_quarter_data:
            team_goals = weekly_quarter_data["teamGoals"]
            key_achievements = self.calculate_key_achievements_from_goals(team_goals)
        else:
            key_achievements = ["주간 평가 데이터 없음"]
        
        # 7. 나머지 컬렉션에서 데이터 수집 (기존 방식)
        peer_data = self.get_data_from_collection("peer_evaluation_results", user_id, year, quarter)
        qualitative_data = self.get_data_from_collection("qualitative_evaluation_results", user_id, year, quarter)
        ranking_data = self.get_data_from_collection("ranking_results", user_id, year, quarter)
        performance_data = self.get_data_from_collection("final_performance_reviews", user_id, year, quarter)
        
        # 8. 최종 점수 조회
        final_score = self.get_final_score(user_id, year, quarter)
        
        # 9. 퍼센타일 텍스트 계산
        compare_text = "데이터 없음"
        if ranking_data and ranking_data.get("ranking_info"):
            rank_info = ranking_data["ranking_info"]
            same_job_rank = rank_info.get("same_job_rank", 0)
            same_job_count = rank_info.get("same_job_user_count", 0)
            if same_job_rank and same_job_count:
                compare_text = self.calculate_percentile_text(same_job_rank, same_job_count)
        
        # 10. 동료 피드백 정리
        peer_feedback = []
        if peer_data and peer_data.get("keyword_summary"):
            keyword_summary = peer_data["keyword_summary"]
            
            if keyword_summary.get("positive"):
                peer_feedback.append({
                    "type": "positive",
                    "keywords": keyword_summary["positive"]
                })
            
            if keyword_summary.get("negative"):
                peer_feedback.append({
                    "type": "negative", 
                    "keywords": keyword_summary["negative"]
                })
        
        # 11. 종합 리포트 구성
        report = {
            "type": "personal-quarter",
            "evaluated_year": year,
            "evaluated_quarter": quarter,
            "created_at": datetime.now().strftime("%Y-%m-%d"),
            "title": f"{year} {quarter}분기 성과 리포트",
            "startDate": start_date,
            "endDate": end_date,
            "user": {
                "userId": user_id,
                "name": user_info["name"],
                "department": department  # ✅ weekly 데이터에서 추출
            },
            "finalScore": final_score,
            "compareText": compare_text,
            "rank": ranking_data.get("ranking_info", {}) if ranking_data else {},
            "teamGoals": team_goals,  # ✅ weekly 데이터에서 완전한 구조
            "keyAchievements": key_achievements,  # ✅ 통계 계산됨
            "peerFeedback": peer_feedback,
            "quarterlyPerformanceSummary": {
                "summaryText": performance_data.get("performance_summary", "") if performance_data else ""
            },
            "workAttitude": qualitative_data.get("work_attitude", []) if qualitative_data else [],
            "finalComment": performance_data.get("performance_summary", "") if performance_data else ""
        }
        
        return report
    
    def save_report_to_quarter_collection(self, report_data: Dict) -> bool:
        """분기별 문서에 사용자 리포트 저장"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db["reports"]
            
            year = report_data["evaluated_year"]
            quarter = report_data["evaluated_quarter"]
            user_id = report_data["user"]["userId"]
            
            # 분기별 문서 찾기
            quarter_document = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": year,
                "evaluated_quarter": quarter
            })
            
            if quarter_document:
                # 기존 분기 문서가 있으면 해당 사용자 데이터 업데이트
                collection.update_one(
                    {
                        "type": "personal-quarter",
                        "evaluated_year": year,
                        "evaluated_quarter": quarter
                    },
                    {
                        "$set": {
                            f"users.{user_id}": report_data,
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                    }
                )
                print(f"✅ {year}Q{quarter} 문서에 사용자 {user_id} 데이터 업데이트 완료")
            else:
                # 새 분기 문서 생성
                quarter_document = {
                    "type": "personal-quarter",
                    "evaluated_year": year,
                    "evaluated_quarter": quarter,
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "title": f"{year} {quarter}분기 성과 리포트 모음",
                    "users": {
                        str(user_id): report_data
                    }
                }
                
                result = collection.insert_one(quarter_document)
                print(f"✅ {year}Q{quarter} 새 문서 생성 및 사용자 {user_id} 데이터 저장 완료 - Document ID: {result.inserted_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ 분기별 리포트 저장 실패: {e}")
            return False
    
    def process_batch_reports(self, user_ids: List[int], year: int, quarter: int) -> List[Dict]:
        """배치 리포트 생성"""
        results = []
        total_users = len(user_ids)
        
        for i, user_id in enumerate(user_ids, 1):
            if i % 10 == 0 or i == total_users:
                print(f"처리 진행률: {i}/{total_users} ({i/total_users*100:.1f}%)")
            
            try:
                # 리포트 생성
                report = self.generate_comprehensive_report(user_id, year, quarter)
                
                # reports 컬렉션에 저장 (분기별 구조)
                save_success = self.save_report_to_quarter_collection(report)
                
                if save_success:
                    results.append({
                        "success": True,
                        "user_id": user_id,
                        "message": "리포트 생성 및 저장 완료"
                    })
                    print(f"✓ User {user_id}: 종합 리포트 생성 완료 → reports 컬렉션에 저장 완료")
                else:
                    results.append({
                        "success": False,
                        "user_id": user_id,
                        "message": "리포트 저장 실패"
                    })
                    print(f"✗ User {user_id}: 리포트 저장 실패")
                
            except Exception as e:
                results.append({
                    "success": False,
                    "user_id": user_id,
                    "message": f"리포트 생성 실패: {str(e)}"
                })
                print(f"✗ User {user_id}: 리포트 생성 실패 - {str(e)}")
        
        return results
    
    def get_quarter_report_summary(self, year: int, quarter: int) -> Dict:
        """분기별 리포트 요약 정보 조회"""
        try:
            if not self.client:
                if not self.connect():
                    return {}
            
            db = self.client[self.database_name]
            collection = db["reports"]
            
            # 분기별 문서 조회
            quarter_document = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": year,
                "evaluated_quarter": quarter
            })
            
            if quarter_document and "users" in quarter_document:
                user_count = len(quarter_document["users"])
                return {
                    "year": year,
                    "quarter": quarter,
                    "total_users": user_count,
                    "document_id": str(quarter_document["_id"]),
                    "created_at": quarter_document.get("created_at", ""),
                    "updated_at": quarter_document.get("updated_at", "")
                }
            else:
                return {
                    "year": year,
                    "quarter": quarter,
                    "total_users": 0,
                    "document_id": None,
                    "created_at": "",
                    "updated_at": ""
                }
                
        except Exception as e:
            print(f"❌ {year}Q{quarter} 리포트 요약 조회 실패: {e}")
            return {}
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()
            print("MongoDB 연결 종료")

def process_single_quarter_reports(generator: ComprehensiveReportGenerator, user_ids: List[int], year: int, quarter: int):
    """단일 분기 종합 리포트 처리"""
    print(f"\n=== {year}년 {quarter}분기 종합 리포트 생성 시작 ===")
    print(f"처리할 사용자 수: {len(user_ids)}명")
    print(f"저장 위치: MongoDB reports 컬렉션")
    print("=" * 50)
    
    # 배치 처리 실행
    results = generator.process_batch_reports(user_ids, year, quarter)
    
    # 결과 통계
    successful_count = sum(1 for r in results if r["success"])
    failed_count = len(results) - successful_count
    
    # 분기별 리포트 요약 조회
    quarter_summary = generator.get_quarter_report_summary(year, quarter)
    
    print(f"\n=== {quarter}분기 종합 리포트 생성 완료 ===")
    print(f"성공: {successful_count}명")
    print(f"실패: {failed_count}명")
    print(f"분기 문서 정보:")
    print(f"  - Document ID: {quarter_summary.get('document_id', 'N/A')}")
    print(f"  - 저장된 사용자 수: {quarter_summary.get('total_users', 0)}명")
    print(f"  - 생성일시: {quarter_summary.get('created_at', 'N/A')}")
    print(f"  - 수정일시: {quarter_summary.get('updated_at', 'N/A')}")
    
    return {
        "quarter": quarter,
        "successful_count": successful_count,
        "failed_count": failed_count,
        "document_summary": quarter_summary
    }

def main():
    print("🚀 종합 성과 리포트 생성 시스템 시작")
    print("=" * 60)
    
    # 리포트 생성기 초기화
    generator = ComprehensiveReportGenerator()
    
    # MongoDB 연결 테스트
    print("🔌 MongoDB 연결 테스트...")
    if not generator.connect():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    # 평가 년도 설정
    evaluation_year = 2024
    
    print(f"\n=== {evaluation_year}년 전체 분기 종합 리포트 생성 시작 ===")
    print(f"데이터 소스:")
    print(f"  - weekly_evaluation_results (주간평가) ✅ 개선됨")
    print(f"  - peer_evaluation_results (동료평가)")
    print(f"  - qualitative_evaluation_results (정성평가)")
    print(f"  - ranking_results (랭킹)")
    print(f"  - final_performance_reviews (성과검토)")
    print(f"  - user_quarter_scores (MariaDB 최종점수)")
    print(f"저장 위치: reports 컬렉션")
    print("=" * 60)
    
    # 처리할 사용자 ID 리스트 (1~100)
    user_ids = list(range(1, 101))
    
    # 전체 결과 저장용
    all_quarters_results = {}
    
    # 4개 분기 모두 처리
    for quarter in [1, 2, 3, 4]:
        quarter_result = process_single_quarter_reports(generator, user_ids, evaluation_year, quarter)
        all_quarters_results[f"Q{quarter}"] = quarter_result
        
        # 백업 파일도 저장
        backup_filename = f"comprehensive_reports_{evaluation_year}Q{quarter}_backup.json"
        with open(backup_filename, 'w', encoding='utf-8') as f:
            json.dump(quarter_result, f, ensure_ascii=False, indent=2)
        print(f"📄 백업 파일 저장 완료: {backup_filename}")
        
        # 분기 간 구분
        print("\n" + "=" * 60)
    
    print(f"\n🎉 {evaluation_year}년 전체 분기 종합 리포트 생성 완료!")
    print("=" * 60)
    
    total_processed = 0
    total_documents = 0
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"]
            total_processed += successful
            
            document_summary = quarter_data.get("document_summary", {})
            if document_summary.get("document_id"):
                total_documents += 1
                print(f"Q{quarter}: {successful}명 성공 → 분기별 문서 1개에 저장 완료")
                print(f"       Document ID: {document_summary['document_id']}")
            else:
                print(f"Q{quarter}: 데이터 없음")
        else:
            print(f"Q{quarter}: 데이터 없음")
    
    print(f"\n🎉 처리 완료 요약:")
    print(f"  - 총 생성된 리포트: {total_processed}개")
    print(f"  - 생성된 분기 문서: {total_documents}개")
    print(f"  - 저장 위치: {MONGO_CONFIG['db_name']}.reports")
    print(f"  - 저장 구조: 분기별 문서 → users.{{user_id}} 형태")
    print(f"  - 리포트 형식: JSON 구조화된 종합 성과 리포트")
    print(f"  - 문서 구조:")
    print(f"    └─ 2024Q1 문서")
    print(f"       ├─ users.1 (사용자 1 리포트)")
    print(f"       ├─ users.2 (사용자 2 리포트)")
    print(f"       └─ users.N (사용자 N 리포트)")
    print(f"  - 포함 데이터:")
    print(f"    • 사용자 기본 정보 (이름, 직무, 부서)")
    print(f"    • 최종 점수 및 상대적 위치")
    print(f"    • 랭킹 정보 (직무별, 팀별)")
    print(f"    • 팀 목표 및 기여도 (상세 내용 포함)")
    print(f"    • 주요 성과 통계 (활동 건수, 참여율)")
    print(f"    • 동료 피드백 (긍정/부정 키워드)")
    print(f"    • 업무 태도 평가")
    print(f"    • AI 생성 성과 요약")
    
    # MongoDB 연결 종료
    generator.close()
    
    return all_quarters_results

if __name__ == "__main__":
    main()