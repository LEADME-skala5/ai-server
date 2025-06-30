import os
import json
import mysql.connector
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
from pymongo import MongoClient

# .env 파일 로드
load_dotenv()

class WebJSONGenerator:
    """웹용 최종 JSON 생성기"""
    
    def __init__(self):
        # MariaDB 설정
        self.db_config = {
            'host': os.getenv("DB_HOST"),
            'port': int(os.getenv("DB_PORT")),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'database': os.getenv("DB_NAME")
        }
        
        # MongoDB 설정
        self.mongo_host = os.getenv("MONGO_HOST")
        self.mongo_port = int(os.getenv("MONGO_PORT"))
        self.mongo_user = os.getenv("MONGO_USER")
        self.mongo_password = os.getenv("MONGO_PASSWORD")
        self.mongo_db_name = os.getenv("MONGO_DB_NAME")
        
        self.mongodb_uri = f"mongodb://{self.mongo_user}:{self.mongo_password}@{self.mongo_host}:{self.mongo_port}/"
        self.mongo_client = None
        
        print(f"📋 웹용 JSON 생성기 초기화 완료")
        print(f"MariaDB: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
        print(f"MongoDB: {self.mongo_host}:{self.mongo_port}/{self.mongo_db_name}")
    
    def get_db_connection(self):
        """MariaDB 연결"""
        return mysql.connector.connect(**self.db_config)
    
    def connect_mongodb(self):
        """MongoDB 연결"""
        try:
            self.mongo_client = MongoClient(self.mongodb_uri)
            self.mongo_client.admin.command('ping')
            print("✅ MongoDB 연결 성공!")
            return True
        except Exception as e:
            print(f"❌ MongoDB 연결 실패: {e}")
            return False
    
    def get_all_users(self) -> List[Dict]:
        """MariaDB users 테이블에서 모든 사용자 조회"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT id, name FROM users ORDER BY id")
            users = cursor.fetchall()
            conn.close()
            
            print(f"✅ 총 {len(users)}명의 사용자 조회 완료")
            return users
        except Exception as e:
            print(f"❌ 사용자 조회 실패: {e}")
            return []
    
    def get_user_department(self, user_id: int) -> str:
        """MariaDB에서 사용자 부서 정보 조회"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT u.name, d.name as department_name
                FROM users u
                LEFT JOIN departments d ON u.department_id = d.id
                WHERE u.id = %s
            """, (user_id,))
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return result.get('department_name', '') or ''
            return ''
        except Exception as e:
            print(f"❌ 사용자 {user_id} 부서 정보 조회 실패: {e}")
            return ''
    
    def get_peer_annual_data(self, user_id: int, year: int) -> Dict:
        """peer_evaluation_results에서 연간 데이터 조회"""
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return {}
            
            db = self.mongo_client[self.mongo_db_name]
            collection = db["peer_evaluation_results"]
            
            # type: "personal-annual" 문서 조회
            document = collection.find_one({
                "type": "personal-annual",
                "evaluated_year": year
            })
            
            if not document or "users" not in document:
                return {}
            
            # 해당 사용자 데이터 찾기
            for user_data in document["users"]:
                if user_data.get("user_id") == user_id:
                    return user_data
            
            return {}
            
        except Exception as e:
            print(f"❌ peer 연간 데이터 조회 실패 (user: {user_id}): {e}")
            return {}
    
    def get_final_score_data(self, user_id: int, year: int) -> Dict:
        """final_score_results에서 최종 점수 데이터 조회"""
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return {}
            
            db = self.mongo_client[self.mongo_db_name]
            collection = db["final_score_results"]
            
            # type: "personal-final-score-annual" 문서 조회
            document = collection.find_one({
                "type": "personal-final-score-annual",
                "evaluated_year": year
            })
            
            if not document:
                return {}
            
            if "users" not in document:
                return {}
            
            users_data = document["users"]
            
            # users가 딕셔너리인 경우 (user_id를 키로 사용)
            if isinstance(users_data, dict):
                user_id_str = str(user_id)
                if user_id_str in users_data:
                    return users_data[user_id_str]
                else:
                    return {}
            
            # users가 배열인 경우 (기존 로직)
            elif isinstance(users_data, list):
                for user_data in users_data:
                    if isinstance(user_data, dict) and user_data.get("user_id") == user_id:
                        return user_data
                return {}
            
            else:
                return {}
            
        except Exception as e:
            print(f"❌ final_score 데이터 조회 실패 (user: {user_id}): {e}")
            return {}
    
    def get_weekly_annual_data(self, user_id: int, year: int) -> Dict:
        """weekly_evaluation_results에서 연간 데이터 조회"""
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return {}
            
            db = self.mongo_client[self.mongo_db_name]
            collection = db["weekly_evaluation_results"]
            
            # data_type: "personal-annual" 문서 조회
            document = collection.find_one({
                "data_type": "personal-annual"
            })
            
            if not document or "users" not in document:
                return {}
            
            # 사용자 ID를 문자열로 변환하여 검색
            user_id_str = str(user_id)
            
            if user_id_str not in document["users"]:
                return {}
            
            user_data = document["users"][user_id_str]
            return user_data.get("annual_report", {})
            
        except Exception as e:
            print(f"❌ weekly 연간 데이터 조회 실패 (user: {user_id}): {e}")
            return {}
    
    def get_final_performance_data(self, user_id: int, year: int) -> Dict:
        """final_performance_reviews에서 종합 Comment 조회"""
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return {}
            
            db = self.mongo_client[self.mongo_db_name]
            collection = db["final_performance_reviews"]
            
            # type: "personal-annual" 문서 조회
            document = collection.find_one({
                "type": "personal-annual",
                "evaluated_year": year
            })
            
            if not document or "users" not in document:
                return {}
            
            # 해당 사용자 데이터 찾기
            for user_data in document["users"]:
                if user_data.get("user_id") == user_id:
                    return user_data
            
            return {}
            
        except Exception as e:
            print(f"❌ final_performance 데이터 조회 실패 (user: {user_id}): {e}")
            return {}
    
    def generate_web_json(self, user_id: int, year: int) -> Dict:
        """개별 사용자의 웹용 JSON 생성"""
        try:
            # 1. 기본 사용자 정보 (peer_evaluation_results에서)
            peer_data = self.get_peer_annual_data(user_id, year)
            user_name = peer_data.get("user_name", f"사용자{user_id}")
            
            # 2. 부서 정보 (MariaDB에서)
            department = self.get_user_department(user_id)
            
            # 3. 점수 정보 (final_score_results에서)
            final_score_data = self.get_final_score_data(user_id, year)
            final_score_info = final_score_data.get("final_score_info", {})
            
            # 실제 점수가 있는 annual_score_averages 사용
            annual_score_averages = final_score_data.get("annual_score_averages", {})
            
            # 최종 점수
            final_score = final_score_info.get("overall_final_score", 0.0)
            
            # 카테고리별 점수 추출 방식
            annual_comment_summaries = final_score_data.get("annual_comment_summaries", {})
            
            # 다양한 가능한 키 조합 시도
            def get_category_score(category_key):
                """카테고리별 점수 추출 (여러 가능한 키 조합 시도)"""
                # 1. annual_score_averages에서 먼저 찾기 (실제 점수가 여기 있음)
                if category_key in annual_score_averages:
                    score_data = annual_score_averages[category_key]
                    if isinstance(score_data, dict):
                        # qualitative_score, peer_evaluation_score 등의 형태로 찾기
                        for score_key in score_data:
                            if "score" in score_key:
                                score_value = score_data[score_key]
                                if isinstance(score_value, (int, float)):
                                    return float(score_value)
                    elif isinstance(score_data, (int, float)):
                        return float(score_data)
                
                # 2. quantitative를 weekly로 매핑
                if category_key == "weekly" or category_key == "quantitative":
                    # quantitative 또는 weekly로 둘 다 시도
                    for alt_key in ["quantitative", "weekly"]:
                        if alt_key in annual_score_averages:
                            score_data = annual_score_averages[alt_key]
                            if isinstance(score_data, dict):
                                for score_key in score_data:
                                    if "score" in score_key:
                                        score_value = score_data[score_key]
                                        if isinstance(score_value, (int, float)):
                                            return float(score_value)
                
                return 0.0
            
            # 카테고리별 점수 및 요약 구성
            value_score = [
                {
                    "category": "weekly",
                    "score": get_category_score("weekly") or get_category_score("quantitative"),
                    "summary": annual_comment_summaries.get("quantitative", "")
                },
                {
                    "category": "qualitative",
                    "score": get_category_score("qualitative"),
                    "summary": annual_comment_summaries.get("qualitative", "")
                },
                {
                    "category": "peer-review",
                    "score": get_category_score("peer") or get_category_score("peer-review"),
                    "summary": annual_comment_summaries.get("peer", "")
                }
            ]
            
            # 4. 분기별 성과 (weekly_evaluation_results에서)
            weekly_data = self.get_weekly_annual_data(user_id, year)
            quarterly_performance = weekly_data.get("quarterlyPerformance", [])
            
            # 5. 주요 성취 (weekly_evaluation_results에서)
            key_achievements = weekly_data.get("keyAchievements", [])
            
            # 6. 동료 피드백 (peer_evaluation_results에서)
            peer_feedback = []
            top_positive_keywords = peer_data.get("top_positive_keywords", [])
            top_negative_keywords = peer_data.get("top_negative_keywords", [])
            
            if top_positive_keywords:
                # 상위 5개만 키워드명만 추출
                positive_keywords = []
                for kw_data in top_positive_keywords[:5]:
                    if isinstance(kw_data, dict):
                        positive_keywords.append(kw_data.get("keyword", ""))
                    else:
                        positive_keywords.append(str(kw_data))
                
                peer_feedback.append({
                    "type": "positive",
                    "keywords": positive_keywords
                })
            
            if top_negative_keywords:
                # 상위 5개만 키워드명만 추출
                negative_keywords = []
                for kw_data in top_negative_keywords[:5]:
                    if isinstance(kw_data, dict):
                        negative_keywords.append(kw_data.get("keyword", ""))
                    else:
                        negative_keywords.append(str(kw_data))
                
                peer_feedback.append({
                    "type": "negative",
                    "keywords": negative_keywords
                })
            
            # 7. 최종 코멘트 (final_performance_reviews에서)
            final_performance_data = self.get_final_performance_data(user_id, year)
            final_comment = final_performance_data.get("comprehensive_comment", "")
            
            # 8. 웹용 JSON 구성
            web_json = {
                "type": "personal-annual",
                "evaluated_year": year,
                "title": f"{year} 연말 성과 리포트",
                "created_at": datetime.now().strftime("%Y-%m-%d"),
                "startDate": f"{year}-01-01",
                "endDate": f"{year}-12-31",
                "user": {
                    "userId": user_id,
                    "name": user_name,
                    "department": department
                },
                "finalScore": round(final_score, 1),
                "valueScore": value_score,
                "quarterlyPerformance": quarterly_performance,
                "keyAchievements": key_achievements,
                "peerFeedback": peer_feedback,
                "finalComment": final_comment
            }
            
            return web_json
            
        except Exception as e:
            print(f"❌ 사용자 {user_id} 웹 JSON 생성 실패: {e}")
            return {}
    
    def save_web_json_to_mongodb(self, web_json_data: Dict, year: int) -> bool:
        """웹용 JSON을 MongoDB reports 컬렉션에 개별 문서로 저장"""
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return False
            
            db = self.mongo_client[self.mongo_db_name]
            collection = db["reports"]
            
            user_id = web_json_data["user"]["userId"]
            
            # 기존 문서 찾기 (같은 사용자, 같은 연도)
            existing_doc = collection.find_one({
                "type": "personal-annual",
                "evaluated_year": year,
                "user.userId": user_id
            })
            
            if existing_doc:
                # 기존 문서 전체 교체
                collection.replace_one(
                    {
                        "type": "personal-annual",
                        "evaluated_year": year,
                        "user.userId": user_id
                    },
                    web_json_data
                )
                print(f"✅ 사용자 ID {user_id} 웹 JSON 문서 업데이트 완료")
            else:
                # 새 문서 생성
                result = collection.insert_one(web_json_data)
                print(f"✅ 사용자 ID {user_id} 새 웹 JSON 문서 생성 완료 - Document ID: {result.inserted_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 웹 JSON 저장 실패 (사용자 ID: {user_id}): {e}")
            return False
    
    def process_all_users_web_json(self, year: int) -> List[Dict]:
        """모든 사용자의 웹용 JSON 처리"""
        # 1. 모든 사용자 조회
        users = self.get_all_users()
        if not users:
            print("❌ 사용자 데이터가 없습니다.")
            return []
        
        results = []
        successful_count = 0
        failed_count = 0
        
        print(f"\n=== {year}년 웹용 JSON 생성 시작 ===")
        print(f"처리할 사용자 수: {len(users)}명")
        print(f"데이터 소스:")
        print(f"  - peer_evaluation_results (기본 정보)")
        print(f"  - MariaDB users (부서 정보)")
        print(f"  - final_score_results (점수 정보)")
        print(f"  - weekly_evaluation_results (분기별 성과, 주요 성취)")
        print(f"  - final_performance_reviews (최종 코멘트)")
        print("=" * 60)
        
        for i, user in enumerate(users, 1):
            user_id = user['id']
            user_name = user['name']
            
            # 진행률 표시
            if i % 10 == 0 or i == len(users) or i == 1:
                print(f"처리 진행률: {i}/{len(users)} ({i/len(users)*100:.1f}%)")
            
            # 개별 사용자 처리
            web_json = self.generate_web_json(user_id, year)
            
            if web_json:
                # MongoDB에 개별 문서로 저장
                save_success = self.save_web_json_to_mongodb(web_json, year)
                
                if save_success:
                    successful_count += 1
                    final_score = web_json.get("finalScore", 0)
                    # valueScore에서 0이 아닌 점수들만 표시
                    value_scores = []
                    for vs in web_json.get("valueScore", []):
                        if vs.get("score", 0) > 0:
                            value_scores.append(f"{vs['category']}:{vs['score']}")
                    value_scores_str = ", ".join(value_scores) if value_scores else "모든 카테고리 0점"
                    
                    print(f"✓ User {user_id} ({user_name}): 최종점수 {final_score}, 카테고리별 [{value_scores_str}] → 개별 웹 JSON 문서 저장 완료")
                    results.append({
                        "success": True,
                        "user_id": user_id,
                        "data": web_json
                    })
                else:
                    failed_count += 1
                    print(f"✗ User {user_id} ({user_name}): JSON 생성 성공, MongoDB 저장 실패")
                    results.append({
                        "success": False,
                        "user_id": user_id,
                        "message": "저장 실패"
                    })
            else:
                failed_count += 1
                print(f"✗ User {user_id} ({user_name}): 웹 JSON 생성 실패")
                results.append({
                    "success": False,
                    "user_id": user_id,
                    "message": "JSON 생성 실패"
                })
        
        print(f"\n=== {year}년 웹용 JSON 생성 완료 ===")
        print(f"성공: {successful_count}명")
        print(f"실패: {failed_count}명")
        print(f"저장 위치: {self.mongo_db_name}.reports")
        print(f"저장 방식: 사용자별 개별 문서 (type='personal-annual', evaluated_year={year})")
        print(f"문서 구조: 각 문서가 완전한 웹 JSON 형태")
        
        return results
    
    def close(self):
        """연결 종료"""
        if self.mongo_client:
            self.mongo_client.close()
            print("MongoDB 연결 종료")

def main():
    print("🚀 웹용 최종 JSON 생성 시스템 시작")
    print("=" * 60)
    
    # 생성기 초기화
    generator = WebJSONGenerator()
    
    # MongoDB 연결 테스트
    if not generator.connect_mongodb():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    # 2024년 웹용 JSON 생성
    evaluation_year = 2024
    
    print(f"\n🎯 {evaluation_year}년 웹용 최종 JSON 생성")
    print(f"생성 형태: 웹 화면 표시용 통합 JSON")
    print(f"포함 내용:")
    print(f"  - 기본 정보 (이름, 부서)")
    print(f"  - 최종 점수 및 카테고리별 점수")
    print(f"  - 분기별 성과 및 주요 성취")
    print(f"  - 동료 피드백 키워드 (상위 5개)")
    print(f"  - AI 생성 최종 코멘트")
    
    # 전체 사용자 처리
    results = generator.process_all_users_web_json(evaluation_year)
    
    # 통계 출력
    successful_results = [r for r in results if r["success"]]
    
    if successful_results:
        final_scores = [r["data"]["finalScore"] for r in successful_results if "data" in r]
        if final_scores:
            avg_score = sum(final_scores) / len(final_scores)
            max_score = max(final_scores)
            min_score = min(final_scores)
            
            print(f"\n📊 점수 통계:")
            print(f"  - 평균 최종 점수: {avg_score:.1f}점")
            print(f"  - 최고 점수: {max_score:.1f}점")
            print(f"  - 최저 점수: {min_score:.1f}점")
        
        # 카테고리별 점수 통계
        weekly_scores = []
        qualitative_scores = []
        peer_scores = []
        
        for result in successful_results:
            if "data" in result:
                value_score = result["data"].get("valueScore", [])
                for vs in value_score:
                    if vs["category"] == "weekly" and vs["score"] > 0:
                        weekly_scores.append(vs["score"])
                    elif vs["category"] == "qualitative" and vs["score"] > 0:
                        qualitative_scores.append(vs["score"])
                    elif vs["category"] == "peer-review" and vs["score"] > 0:
                        peer_scores.append(vs["score"])
        
        print(f"\n📈 카테고리별 점수 통계:")
        if weekly_scores:
            print(f"  - Weekly 평균: {sum(weekly_scores)/len(weekly_scores):.1f}점 ({len(weekly_scores)}명)")
        else:
            print(f"  - Weekly: 데이터 없음")
        
        if qualitative_scores:
            print(f"  - Qualitative 평균: {sum(qualitative_scores)/len(qualitative_scores):.1f}점 ({len(qualitative_scores)}명)")
        else:
            print(f"  - Qualitative: 데이터 없음")
        
        if peer_scores:
            print(f"  - Peer-review 평균: {sum(peer_scores)/len(peer_scores):.1f}점 ({len(peer_scores)}명)")
        else:
            print(f"  - Peer-review: 데이터 없음")
    
    print(f"\n🎉 웹용 JSON 생성 시스템 완료!")
    print(f"📄 결과 확인: MongoDB > {generator.mongo_db_name} > reports 컬렉션")
    print(f"📋 문서 구조: 사용자별 개별 문서, 각각이 완전한 웹 JSON")
    print(f"💾 저장 방식: 사용자 수만큼 개별 문서 생성")
    print(f"🔍 조회 방법: type='personal-annual' AND evaluated_year={evaluation_year} AND user.userId=원하는사용자ID")
    
    # 연결 종료
    generator.close()
    
    return results

if __name__ == "__main__":
    main()