import os
import pymysql
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from pathlib import Path
from pymongo import MongoClient
from statistics import mean

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

class AnnualEvaluationAgent:
    """연말 평가 에이전트 - 1~4분기 데이터 종합 분석"""
    
    def __init__(self):
        self.mongodb_uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        self.database_name = MONGO_CONFIG["db_name"]
        self.client = None
        
        print(f"📊 연말 평가 에이전트 초기화 완료")
        print(f"MongoDB: {MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{self.database_name}")
    
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
    
    def get_all_user_ids(self) -> List[int]:
        """users 테이블의 모든 사용자 ID 목록 조회"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM users 
                    ORDER BY id
                """)
                results = cur.fetchall()
                user_ids = [row['id'] for row in results]
                print(f"✅ users 테이블에서 {len(user_ids)}명의 사용자 조회 완료")
                if user_ids:
                    print(f"사용자 ID 범위: {min(user_ids)} ~ {max(user_ids)}")
                return user_ids
        except Exception as e:
            print(f"❌ 사용자 ID 목록 조회 실패: {e}")
            return []
        finally:
            if 'conn' in locals():
                conn.close()
    
    def get_user_basic_info(self, user_id: int) -> Dict:
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
    
    def get_quarterly_data_from_collection(self, collection_name: str, user_id: int, year: int, quarter: int) -> Optional[Dict]:
        """특정 컬렉션에서 사용자의 분기별 데이터 조회"""
        try:
            if not self.client:
                if not self.connect():
                    return None
            
            db = self.client[self.database_name]
            collection = db[collection_name]
            
            # 분기별 문서 조회
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
            print(f"❌ {collection_name} 데이터 조회 실패 (user: {user_id}, {year}Q{quarter}): {e}")
            return None
    
    def get_user_annual_data(self, user_id: int, year: int) -> Dict:
        """사용자의 연간 평가 데이터 수집 (1~4분기)"""
        print(f"🔍 사용자 {user_id}의 {year}년 연간 데이터 수집 중...")
        
        annual_data = {
            "user_id": user_id,
            "year": year,
            "quarterly_data": {
                "quantitative": {},  # weekly_combination_results
                "qualitative": {},   # qualitative_evaluation_results
                "peer": {}          # peer_evaluation_results
            }
        }
        
        # 1~4분기 데이터 수집
        for quarter in [1, 2, 3, 4]:
            quarter_key = f"Q{quarter}"
            
            # 정량 평가 데이터
            quantitative_data = self.get_quarterly_data_from_collection(
                "weekly_combination_results", user_id, year, quarter
            )
            if quantitative_data:
                annual_data["quarterly_data"]["quantitative"][quarter_key] = quantitative_data
            
            # 정성 평가 데이터
            qualitative_data = self.get_quarterly_data_from_collection(
                "qualitative_evaluation_results", user_id, year, quarter
            )
            if qualitative_data:
                annual_data["quarterly_data"]["qualitative"][quarter_key] = qualitative_data
            
            # 동료 평가 데이터
            peer_data = self.get_quarterly_data_from_collection(
                "peer_evaluation_results", user_id, year, quarter
            )
            if peer_data:
                annual_data["quarterly_data"]["peer"][quarter_key] = peer_data
        
        # 데이터 수집 현황 로그
        q_count = len(annual_data["quarterly_data"]["quantitative"])
        qual_count = len(annual_data["quarterly_data"]["qualitative"])
        peer_count = len(annual_data["quarterly_data"]["peer"])
        
        print(f"✅ 사용자 {user_id} 데이터 수집 완료: 정량({q_count}분기), 정성({qual_count}분기), 동료({peer_count}분기)")
        
        return annual_data
    
    def calculate_annual_score_averages(self, quarterly_data: Dict) -> Dict:
        """각 평가 항목별 연간 평균 점수 계산"""
        score_averages = {
            "quantitative": {},
            "qualitative": {},
            "peer": {}
        }
        
        # 정량 평가 점수 평균 계산
        if "quantitative" in quarterly_data:
            quantitative_scores = {}
            for quarter, data in quarterly_data["quantitative"].items():
                if isinstance(data, dict) and "scores" in data:
                    for metric, score in data["scores"].items():
                        if metric not in quantitative_scores:
                            quantitative_scores[metric] = []
                        if isinstance(score, (int, float)):
                            quantitative_scores[metric].append(score)
            
            # 평균 계산
            for metric, scores in quantitative_scores.items():
                if scores:
                    score_averages["quantitative"][metric] = round(mean(scores), 2)
        
        # 정성 평가 점수 평균 계산
        if "qualitative" in quarterly_data:
            qualitative_scores = {}
            for quarter, data in quarterly_data["qualitative"].items():
                if isinstance(data, dict) and "evaluation_scores" in data:
                    for metric, score in data["evaluation_scores"].items():
                        if metric not in qualitative_scores:
                            qualitative_scores[metric] = []
                        if isinstance(score, (int, float)):
                            qualitative_scores[metric].append(score)
            
            # 평균 계산
            for metric, scores in qualitative_scores.items():
                if scores:
                    score_averages["qualitative"][metric] = round(mean(scores), 2)
        
        # 동료 평가 점수 평균 계산
        if "peer" in quarterly_data:
            peer_scores = {}
            for quarter, data in quarterly_data["peer"].items():
                if isinstance(data, dict) and "peer_scores" in data:
                    for metric, score in data["peer_scores"].items():
                        if metric not in peer_scores:
                            peer_scores[metric] = []
                        if isinstance(score, (int, float)):
                            peer_scores[metric].append(score)
            
            # 평균 계산
            for metric, scores in peer_scores.items():
                if scores:
                    score_averages["peer"][metric] = round(mean(scores), 2)
        
        return score_averages
    
    def generate_annual_comment_summary(self, quarterly_data: Dict) -> Dict:
        """각 평가 항목별 연간 코멘트 요약 생성"""
        comment_summaries = {
            "quantitative": "",
            "qualitative": "",
            "peer": ""
        }
        
        # 정량 평가 코멘트 요약
        if "quantitative" in quarterly_data:
            quantitative_comments = []
            for quarter, data in quarterly_data["quantitative"].items():
                if isinstance(data, dict) and "comments" in data:
                    if isinstance(data["comments"], str) and data["comments"].strip():
                        quantitative_comments.append(data["comments"].strip())
                    elif isinstance(data["comments"], list):
                        quantitative_comments.extend([c for c in data["comments"] if isinstance(c, str) and c.strip()])
            
            if quantitative_comments:
                # 간단한 키워드 기반 요약 (실제로는 AI 요약 서비스 사용 권장)
                comment_summaries["quantitative"] = self.create_simple_summary(quantitative_comments, "정량 평가")
        
        # 정성 평가 코멘트 요약
        if "qualitative" in quarterly_data:
            qualitative_comments = []
            for quarter, data in quarterly_data["qualitative"].items():
                if isinstance(data, dict) and "feedback" in data:
                    if isinstance(data["feedback"], str) and data["feedback"].strip():
                        qualitative_comments.append(data["feedback"].strip())
                    elif isinstance(data["feedback"], list):
                        qualitative_comments.extend([c for c in data["feedback"] if isinstance(c, str) and c.strip()])
            
            if qualitative_comments:
                comment_summaries["qualitative"] = self.create_simple_summary(qualitative_comments, "정성 평가")
        
        # 동료 평가 코멘트 요약
        if "peer" in quarterly_data:
            peer_comments = []
            for quarter, data in quarterly_data["peer"].items():
                if isinstance(data, dict) and "peer_feedback" in data:
                    if isinstance(data["peer_feedback"], str) and data["peer_feedback"].strip():
                        peer_comments.append(data["peer_feedback"].strip())
                    elif isinstance(data["peer_feedback"], list):
                        peer_comments.extend([c for c in data["peer_feedback"] if isinstance(c, str) and c.strip()])
            
            if peer_comments:
                comment_summaries["peer"] = self.create_simple_summary(peer_comments, "동료 평가")
        
        return comment_summaries
    
    def create_simple_summary(self, comments: List[str], evaluation_type: str) -> str:
        """단순 키워드 기반 코멘트 요약 생성"""
        if not comments:
            return f"{evaluation_type}에서 특별한 피드백이 없었습니다."
        
        # 모든 코멘트 결합
        combined_text = " ".join(comments)
        
        # 간단한 키워드 빈도 기반 요약 (실제로는 AI 요약 서비스 권장)
        positive_keywords = ["우수", "뛰어남", "성과", "달성", "개선", "향상", "좋음", "만족"]
        negative_keywords = ["부족", "미흡", "개선필요", "아쉬움", "부진", "저조"]
        
        positive_count = sum(1 for keyword in positive_keywords if keyword in combined_text)
        negative_count = sum(1 for keyword in negative_keywords if keyword in combined_text)
        
        if positive_count > negative_count:
            return f"{evaluation_type}에서 전반적으로 우수한 성과를 보였으며, 지속적인 개선과 발전을 이뤄냈습니다."
        elif negative_count > positive_count:
            return f"{evaluation_type}에서 일부 개선이 필요한 영역이 있으나, 지속적인 노력을 통해 발전 가능성을 보였습니다."
        else:
            return f"{evaluation_type}에서 안정적인 성과를 유지하며, 꾸준한 업무 수행을 보여주었습니다."
    
    def calculate_final_annual_score(self, score_averages: Dict) -> Dict:
        """동료, 정성, 정량 평가의 평균 점수들을 다시 평균내서 최종 점수 계산"""
        final_score_info = {
            "category_averages": {},
            "overall_final_score": 0.0,
            "score_breakdown": {}
        }
        
        # 각 카테고리별 전체 평균 계산
        category_totals = {}
        
        for category in ["quantitative", "qualitative", "peer"]:
            if category in score_averages and score_averages[category]:
                scores = list(score_averages[category].values())
                # 해당 카테고리의 모든 항목 평균
                category_average = round(mean(scores), 2)
                category_totals[category] = category_average
                final_score_info["category_averages"][category] = category_average
                final_score_info["score_breakdown"][category] = {
                    "individual_scores": score_averages[category],
                    "category_average": category_average,
                    "score_count": len(scores)
                }
        
        # 최종 점수 = 3개 카테고리 평균의 평균
        if category_totals:
            overall_score = round(mean(list(category_totals.values())), 2)
            final_score_info["overall_final_score"] = overall_score
        
        return final_score_info
    
    def generate_annual_evaluation_report(self, user_id: int, year: int) -> Dict:
        """사용자의 연간 종합 평가 리포트 생성"""
        print(f"📊 사용자 {user_id}의 {year}년 연간 종합 평가 리포트 생성 중...")
        
        # 1. 사용자 기본 정보 조회
        user_info = self.get_user_basic_info(user_id)
        
        # 2. 연간 데이터 수집
        annual_data = self.get_user_annual_data(user_id, year)
        
        # 3. 점수 평균 계산
        score_averages = self.calculate_annual_score_averages(annual_data["quarterly_data"])
        
        # 4. 코멘트 요약 생성
        comment_summaries = self.generate_annual_comment_summary(annual_data["quarterly_data"])
        
        # 5. 최종 점수 계산
        final_score_info = self.calculate_final_annual_score(score_averages)
        
        # 6. 연간 종합 리포트 구성
        annual_report = {
            "type": "personal-annual",
            "evaluated_year": year,
            "created_at": datetime.now().strftime("%Y-%m-%d"),
            "title": f"{year}년 연간 종합 성과 평가",
            "user": {
                "userId": user_id,
                "name": user_info["name"],
                "job_name": user_info["job_name"],
                "job_years": user_info["job_years"]
            },
            "data_coverage": {
                "quantitative_quarters": len(annual_data["quarterly_data"]["quantitative"]),
                "qualitative_quarters": len(annual_data["quarterly_data"]["qualitative"]),
                "peer_quarters": len(annual_data["quarterly_data"]["peer"])
            },
            "annual_score_averages": score_averages,
            "annual_comment_summaries": comment_summaries,
            "raw_quarterly_data": annual_data["quarterly_data"]  # 원본 데이터 보존
        }
        
        print(f"✅ 사용자 {user_id} 연간 리포트 생성 완료 (최종점수: {final_score_info['overall_final_score']})")
        return annual_report
    
    def save_annual_report_to_collection(self, report_data: Dict) -> bool:
        """연간 평가 리포트를 final_score_results 컬렉션에 저장"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db["final_score_results"]
            
            year = report_data["evaluated_year"]
            user_id = report_data["user"]["userId"]
            
            # 연간 최종 점수 문서 찾기
            annual_document = collection.find_one({
                "type": "personal-final-score-annual",
                "evaluated_year": year
            })
            
            if annual_document:
                # 기존 연간 문서가 있으면 해당 사용자 데이터 업데이트
                collection.update_one(
                    {
                        "type": "personal-final-score-annual",
                        "evaluated_year": year
                    },
                    {
                        "$set": {
                            f"users.{user_id}": report_data,
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                    }
                )
                print(f"✅ {year}년 연간 최종점수 문서에 사용자 {user_id} 데이터 업데이트 완료")
            else:
                # 새 연간 문서 생성
                annual_document = {
                    "type": "personal-final-score-annual",
                    "evaluated_year": year,
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "title": f"{year}년 연간 최종 점수 평가 모음",
                    "users": {
                        str(user_id): report_data
                    }
                }
                
                result = collection.insert_one(annual_document)
                print(f"✅ {year}년 연간 최종점수 새 문서 생성 및 사용자 {user_id} 데이터 저장 완료 - Document ID: {result.inserted_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ 연간 최종점수 저장 실패: {e}")
            return False
    
    def process_annual_evaluations(self, user_ids: List[int], year: int) -> List[Dict]:
        """연간 평가 배치 처리"""
        results = []
        total_users = len(user_ids)
        
        print(f"\n🚀 {year}년 연간 평가 배치 처리 시작 (총 {total_users}명)")
        print("=" * 60)
        
        for i, user_id in enumerate(user_ids, 1):
            if i % 10 == 0 or i == total_users:
                print(f"처리 진행률: {i}/{total_users} ({i/total_users*100:.1f}%)")
            
            try:
                # 연간 평가 리포트 생성
                annual_report = self.generate_annual_evaluation_report(user_id, year)
                
                # final_score_results 컬렉션에 저장
                save_success = self.save_annual_report_to_collection(annual_report)
                
                if save_success:
                    results.append({
                        "success": True,
                        "user_id": user_id,
                        "message": "연간 평가 리포트 생성 및 저장 완료"
                    })
                    print(f"✓ User {user_id}: 연간 평가 완료")
                else:
                    results.append({
                        "success": False,
                        "user_id": user_id,
                        "message": "연간 리포트 저장 실패"
                    })
                    print(f"✗ User {user_id}: 저장 실패")
                
            except Exception as e:
                results.append({
                    "success": False,
                    "user_id": user_id,
                    "message": f"연간 평가 처리 실패: {str(e)}"
                })
                print(f"✗ User {user_id}: 처리 실패 - {str(e)}")
        
        return results
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()
            print("MongoDB 연결 종료")

def main():
    print("🎯 연말 평가 에이전트 시작")
    print("=" * 60)
    
    # 에이전트 초기화
    agent = AnnualEvaluationAgent()
    
    # MongoDB 연결 테스트
    print("🔌 MongoDB 연결 테스트...")
    if not agent.connect():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    # 평가 년도 설정
    evaluation_year = 2024
    
    # 평가 대상자 조회
    print(f"\n🔍 {evaluation_year}년 연간 평가 대상자 조회 중...")
    user_ids = agent.get_all_user_ids()
    
    if not user_ids:
        print("❌ 사용자를 찾을 수 없습니다. 프로그램을 종료합니다.")
        agent.close()
        return
    
    print(f"✅ 최종 평가 대상자: {len(user_ids)}명")
    
    # 연간 평가 배치 처리
    results = agent.process_annual_evaluations(user_ids, evaluation_year)
    
    # 결과 통계
    successful_count = sum(1 for r in results if r["success"])
    failed_count = len(results) - successful_count
    
    print(f"\n🎉 {evaluation_year}년 연간 평가 완료!")
    print("=" * 60)
    print(f"성공: {successful_count}명")
    print(f"실패: {failed_count}명")
    print(f"저장 위치: {MONGO_CONFIG['db_name']}.annual_reports")
    print(f"저장 구조: {evaluation_year}년 연간 문서 → users.{{user_id}} 형태")
    
    # 백업 파일 저장
    backup_filename = f"annual_evaluation_results_{evaluation_year}_backup.json"
    backup_data = {
        "year": evaluation_year,
        "total_users": len(user_ids),
        "successful_count": successful_count,
        "failed_count": failed_count,
        "results": results
    }
    
    with open(backup_filename, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, ensure_ascii=False, indent=2)
    print(f"📄 백업 파일 저장 완료: {backup_filename}")
    
    # MongoDB 연결 종료
    agent.close()
    
    return results

if __name__ == "__main__":
    main()