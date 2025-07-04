import os
import pymysql
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from pathlib import Path
from pymongo import MongoClient
from statistics import mean
import openai

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
        
        # OpenAI API 설정
        openai.api_key = os.getenv("OPENAI_API_KEY")
        
        print(f"📊 연말 평가 에이전트 초기화 완료")
        print(f"MongoDB: {MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{self.database_name}")
        print(f"OpenAI API: {'설정됨' if openai.api_key else '설정 안됨'}")
    
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
        
        # 정량 평가 점수 평균 계산 (weekly_score)
        if "quantitative" in quarterly_data:
            weekly_scores = []
            for quarter, data in quarterly_data["quantitative"].items():
                if isinstance(data, dict) and "weekly_score" in data:
                    score = data["weekly_score"]
                    if isinstance(score, (int, float)):
                        weekly_scores.append(score)
            
            if weekly_scores:
                score_averages["quantitative"]["weekly_score"] = round(mean(weekly_scores), 2)
                print(f"    📊 정량 평가 평균: {score_averages['quantitative']['weekly_score']} (분기별: {weekly_scores})")
        
        # 정성 평가 점수 평균 계산 (qualitative_score)
        if "qualitative" in quarterly_data:
            qualitative_scores = []
            for quarter, data in quarterly_data["qualitative"].items():
                if isinstance(data, dict) and "qualitative_score" in data:
                    score = data["qualitative_score"]
                    if isinstance(score, (int, float)):
                        qualitative_scores.append(score)
            
            if qualitative_scores:
                score_averages["qualitative"]["qualitative_score"] = round(mean(qualitative_scores), 2)
                print(f"    📊 정성 평가 평균: {score_averages['qualitative']['qualitative_score']} (분기별: {qualitative_scores})")
        
        # 동료 평가 점수 평균 계산 (peer_evaluation_score)
        if "peer" in quarterly_data:
            peer_scores = []
            for quarter, data in quarterly_data["peer"].items():
                if isinstance(data, dict) and "peer_evaluation_score" in data:
                    score = data["peer_evaluation_score"]
                    if isinstance(score, (int, float)):
                        peer_scores.append(score)
            
            if peer_scores:
                score_averages["peer"]["peer_evaluation_score"] = round(mean(peer_scores), 2)
                print(f"    📊 동료 평가 평균: {score_averages['peer']['peer_evaluation_score']} (분기별: {peer_scores})")
        
        return score_averages
    
    def generate_annual_comment_summary(self, quarterly_data: Dict, user_name: str) -> Dict:
        """각 평가 항목별 연간 코멘트 요약 생성 (사용자 이름 포함)"""
        comment_summaries = {
            "quantitative": "",
            "qualitative": "",
            "peer": ""
        }
        
        # 정량 평가 코멘트 요약
        if "quantitative" in quarterly_data:
            quantitative_comments = []
            for quarter, data in quarterly_data["quantitative"].items():
                if isinstance(data, dict) and "weekly_summary_text" in data:
                    if data["weekly_summary_text"] and data["weekly_summary_text"].strip():
                        quantitative_comments.append(data["weekly_summary_text"].strip())
            
            if quantitative_comments:
                comment_summaries["quantitative"] = self.create_data_driven_summary(quantitative_comments, "quantitative", user_name)
        
        # 정성 평가 코멘트 요약
        if "qualitative" in quarterly_data:
            qualitative_comments = []
            for quarter, data in quarterly_data["qualitative"].items():
                if isinstance(data, dict) and "evaluation_text" in data:
                    if data["evaluation_text"] and data["evaluation_text"].strip():
                        qualitative_comments.append(data["evaluation_text"].strip())
            
            if qualitative_comments:
                comment_summaries["qualitative"] = self.create_data_driven_summary(qualitative_comments, "qualitative", user_name)
        
        # 동료 평가 코멘트 요약
        if "peer" in quarterly_data:
            peer_comments = []
            for quarter, data in quarterly_data["peer"].items():
                if isinstance(data, dict) and "feedback" in data:
                    if data["feedback"] and data["feedback"].strip():
                        peer_comments.append(data["feedback"].strip())
            
            if peer_comments:
                comment_summaries["peer"] = self.create_data_driven_summary(peer_comments, "peer", user_name)
        
        return comment_summaries
    
    def create_data_driven_summary(self, comments: List[str], evaluation_type: str, user_name: str) -> str:
        """개선된 데이터 기반 GPT-4o 요약 생성 (사용자 이름 포함, 논리적 일관성 보장)"""
        if not comments:
            return f"{evaluation_type} 평가에서 특별한 피드백이 없었습니다."
        
        # 보수적이고 논리적으로 일관된 프롬프트 설정
        prompts = {
            "quantitative": {
                "role": "성과 데이터 분석 전문가",
                "instruction": """위 성과 데이터를 분석하여 자연스러운 문장으로 요약해주세요.

분석 원칙:
- 실제 데이터에 언급된 성과 트렌드만 사용
- 성과 패턴과 구체적 성과가 논리적으로 일치해야 함
- 추측하지 말고 실제 언급된 수치나 성과만 사용

성과 패턴별 구조:
1. 변화가 있는 경우 (상승/하락/변동):
   "{user_name}님은 이번 연도 동안 [성과 변화 패턴]을 보였습니다. [특정 분기]에는 특히 [구체적 성과]를 달성했습니다."

2. 일정한 수준 유지 경우:
   "{user_name}님은 이번 연도 동안 일정한 수준의 성과를 유지했으며, [실제 데이터에 언급된 구체적 업무/성과]에서 꾸준한 모습을 보여주었습니다."

주의사항:
- 일정한 수준 유지 시 "특히" 사용 금지
- 성과 수준과 구체적 성과의 논리적 일치 필수
- 실제 평가 데이터에 없는 내용 추가 금지

요약:""",
                "example": f"{user_name}님은 이번 연도 동안 점진적인 성과 향상을 보였습니다. 4분기에는 특히 API 성능 최적화를 통해 시스템 속도를 대폭 개선하는 성과를 달성했습니다."
            },
            
            "qualitative": {
                "role": "행동 분석 전문가",
                "instruction": """위 평가 데이터에서 실제로 언급된 행동 특성만을 바탕으로 자연스러운 문장으로 요약해주세요.

분석 원칙:
- 평가 데이터에 직접 언급된 구체적 행동과 태도만 사용
- 추측하거나 일반화하지 말고 실제 기록된 내용만 활용
- 1~4분기에 반복적으로 나타나는 행동 패턴 위주로 서술
- 과도한 해석이나 추론 금지

문장 구조:
"{user_name}님은 연간 업무 수행에서 [실제 언급된 행동 특성]을 보여주었으며, [구체적 행동 사례]에 [참여/노력/집중]하는 등 [실제 나타난 특성]을 나타냈습니다."

주의사항:
- 긍정적 내용과 부정적 내용을 한 문장에 섞지 마세요
- 실제 평가 데이터에 없는 행동 특성 추가 금지

요약:""",
                "example": f"{user_name}님은 연간 업무 수행에서 체계적이고 신중한 특성을 보여주었으며, 기술 세미나 주도에 적극적으로 참여하고 동료 멘토링을 지속하는 등 자기계발과 지식 공유에 대한 강한 성장 의지를 나타냈습니다."
            },
            
            "peer": {
                "role": "동료 관계 분석 전문가",
                "instruction": """위 동료 평가 데이터에서 실제로 언급된 내용만을 바탕으로 자연스러운 문장으로 요약해주세요.

분석 원칙:
- 동료들이 실제로 언급한 구체적 행동이나 능력만 사용
- 1~4분기에 반복적으로 등장하는 긍정적 평가 위주로 서술
- 추측하지 말고 평가 데이터에 나타난 사실만 기반으로 작성
- 무리한 마무리 문장 만들지 말고 실제 평가 내용으로 자연스럽게 끝내기

문장 구조:
"동료들은 {user_name}님의 [실제 언급된 구체적 행동/능력]을 높이 평가했습니다."
또는
"동료들은 {user_name}님의 [실제 언급된 구체적 행동/능력]을 지속적으로 칭찬했습니다."

주의사항:
- "신뢰를 얻고 있습니다" 같은 뻔한 마무리 금지
- "~에 기여하고 있습니다" 같은 일반적 표현 금지
- 실제 동료 평가 데이터 내용 그대로 활용하여 자연스럽게 종료

요약:""",
                "example": f"동료들은 {user_name}님의 업무 전문성과 깊이 있는 지식 공유를 높이 평가했습니다."
            }
        }
        
        config = prompts[evaluation_type]
        combined_text = " ".join(comments)
        
        # GPT-4o를 사용한 보수적 요약 생성
        try:
            prompt = f"""
당신은 {config['role']}입니다.

다음은 {user_name}님의 1~4분기 {evaluation_type} 평가 데이터입니다:
{combined_text}

{config['instruction']}

참고 예시: "{config['example']}"

중요: 
1. 반드시 "{user_name}님"으로 시작하는 문장을 작성하세요.
2. 평가 데이터에 실제로 나타난 내용만 사용하고, 추측하거나 과도하게 해석하지 마세요.
3. 논리적 일관성을 유지하세요.

자연스러운 문장으로 요약:
"""
            
            response = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": f"당신은 {config['role']}입니다. 평가 데이터에 실제로 나타난 내용만을 기반으로 보수적이고 정확하게 요약해주세요. 추측하지 말고 데이터에 명시된 사실만 사용하며, 논리적 일관성을 유지하세요."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=250,
                temperature=0.0  # 일관된 결과를 위해 0으로 고정
            )
            
            summary = response.choices[0].message.content.strip()
            print(f"✅ {evaluation_type} 보수적 요약 생성 완료: {summary[:50]}...")
            return summary
            
        except Exception as e:
            print(f"❌ GPT 요약 생성 실패 ({evaluation_type}): {e}")
            # 폴백: 보수적 키워드 기반 요약
            return self.create_conservative_fallback_summary(combined_text, evaluation_type, user_name)
    
    def create_conservative_fallback_summary(self, combined_text: str, evaluation_type: str, user_name: str) -> str:
        """GPT 실패 시 보수적 폴백 요약 생성 (사용자 이름 포함)"""
        evaluation_names = {
            "quantitative": "정량",
            "qualitative": "정성", 
            "peer": "동료"
        }
        
        positive_keywords = ["우수", "뛰어남", "성과", "달성", "개선", "향상", "좋음", "만족", "탁월"]
        negative_keywords = ["부족", "미흡", "개선필요", "아쉬움", "부진", "저조"]
        
        positive_count = sum(1 for keyword in positive_keywords if keyword in combined_text)
        negative_count = sum(1 for keyword in negative_keywords if keyword in combined_text)
        
        eval_name = evaluation_names[evaluation_type]
        
        if positive_count > negative_count:
            return f"{user_name}님은 연간 {eval_name} 평가에서 지속적으로 우수한 성과를 보여주었으며, 업무 전문성과 개선 노력에서 뛰어난 모습을 나타냈습니다."
        elif negative_count > positive_count:
            return f"{user_name}님은 연간 {eval_name} 평가에서 일부 개선이 필요한 영역이 있었지만, 꾸준한 노력을 통해 발전 가능성을 보여주었습니다."
        else:
            return f"{user_name}님은 연간 {eval_name} 평가에서 안정적이고 꾸준한 성과를 유지하며, 지속적인 업무 수행 능력을 보여주었습니다."
    
    def calculate_final_annual_score(self, score_averages: Dict) -> Dict:
        """동료, 정성, 정량 평가의 평균 점수들을 다시 평균내서 최종 점수 계산"""
        final_score_info = {
            "category_averages": {},
            "overall_final_score": 0.0,
            "score_breakdown": {},
            "available_categories": []
        }
        
        # 각 카테고리의 연간 평균 점수 추출
        category_scores = []
        
        # 정량 평가 평균 (weekly_score의 연간 평균)
        if "quantitative" in score_averages and score_averages["quantitative"]:
            quantitative_avg = score_averages["quantitative"].get("weekly_score")
            if quantitative_avg is not None:
                category_scores.append(quantitative_avg)
                final_score_info["category_averages"]["quantitative"] = quantitative_avg
                final_score_info["available_categories"].append("quantitative")
                final_score_info["score_breakdown"]["quantitative"] = {
                    "category_average": quantitative_avg,
                    "score_type": "weekly_score"
                }
        
        # 정성 평가 평균 (qualitative_score의 연간 평균)
        if "qualitative" in score_averages and score_averages["qualitative"]:
            qualitative_avg = score_averages["qualitative"].get("qualitative_score")
            if qualitative_avg is not None:
                category_scores.append(qualitative_avg)
                final_score_info["category_averages"]["qualitative"] = qualitative_avg
                final_score_info["available_categories"].append("qualitative")
                final_score_info["score_breakdown"]["qualitative"] = {
                    "category_average": qualitative_avg,
                    "score_type": "qualitative_score"
                }
        
        # 동료 평가 평균 (peer_evaluation_score의 연간 평균)
        if "peer" in score_averages and score_averages["peer"]:
            peer_avg = score_averages["peer"].get("peer_evaluation_score")
            if peer_avg is not None:
                category_scores.append(peer_avg)
                final_score_info["category_averages"]["peer"] = peer_avg
                final_score_info["available_categories"].append("peer")
                final_score_info["score_breakdown"]["peer"] = {
                    "category_average": peer_avg,
                    "score_type": "peer_evaluation_score"
                }
        
        # 최종 점수 = 사용 가능한 카테고리 평균점수들의 평균
        if category_scores:
            overall_score = round(mean(category_scores), 2)
            final_score_info["overall_final_score"] = overall_score
            
            print(f"    📊 최종 점수 계산: {final_score_info['available_categories']} → {category_scores} → 평균 {overall_score}")
        else:
            print(f"    ⚠️  모든 카테고리에 점수 데이터가 없음")
            final_score_info["overall_final_score"] = 0.0
        
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
        
        # 4. 코멘트 요약 생성 (사용자 이름 포함)
        comment_summaries = self.generate_annual_comment_summary(annual_data["quarterly_data"], user_info["name"])
        
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
            "final_score_info": final_score_info,  
            "annual_score_averages": score_averages,
            "annual_comment_summaries": comment_summaries,
            "raw_quarterly_data": annual_data["quarterly_data"]  # 원본 데이터 보존
        }
        
        return annual_report
    
    def save_final_score_to_mariadb(self, user_id: int, year: int, final_score: float) -> bool:
        """MariaDB user_year_scores 테이블에 최종 점수 저장"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cur:
                # 기존 데이터 확인
                cur.execute("""
                    SELECT id FROM user_year_scores 
                    WHERE user_id = %s AND evaluation_year = %s
                """, (user_id, year))
                existing = cur.fetchone()
                
                if existing:
                    # 기존 데이터 업데이트 (랭킹은 나중에 별도 계산)
                    cur.execute("""
                        UPDATE user_year_scores 
                        SET final_score = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND evaluation_year = %s
                    """, (final_score, user_id, year))
                    print(f"✅ MariaDB: 사용자 {user_id} {year}년 최종점수 업데이트 완료 ({final_score})")
                else:
                    # 새 데이터 삽입 (랭킹은 NULL로 초기화, 나중에 별도 계산)
                    cur.execute("""
                        INSERT INTO user_year_scores (user_id, evaluation_year, final_score, user_rank, team_rank, created_at, updated_at)
                        VALUES (%s, %s, %s, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """, (user_id, year, final_score))
                    print(f"✅ MariaDB: 사용자 {user_id} {year}년 최종점수 신규 저장 완료 ({final_score})")
                
                return True
                
        except Exception as e:
            print(f"❌ MariaDB 최종점수 저장 실패 (user: {user_id}): {e}")
            return False
        finally:
            if 'conn' in locals():
                conn.close()
    
    def calculate_and_update_rankings(self, year: int) -> bool:
        """연도별 사용자 랭킹 계산 및 업데이트"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cur:
                print(f"🏆 {year}년 사용자 랭킹 계산 중...")
                
                # 전체 사용자 랭킹 계산 및 업데이트
                cur.execute("""
                    UPDATE user_year_scores u1
                    SET user_rank = (
                        SELECT COUNT(*) + 1
                        FROM user_year_scores u2 
                        WHERE u2.evaluation_year = %s 
                        AND u2.final_score > u1.final_score
                        AND u2.final_score IS NOT NULL
                    )
                    WHERE u1.evaluation_year = %s AND u1.final_score IS NOT NULL
                """, (year, year))
                
                # 팀별 랭킹 계산 및 업데이트 (users 테이블과 조인)
                cur.execute("""
                    UPDATE user_year_scores uys
                    INNER JOIN users u ON uys.user_id = u.id
                    SET uys.team_rank = (
                        SELECT COUNT(*) + 1
                        FROM user_year_scores uys2
                        INNER JOIN users u2 ON uys2.user_id = u2.id
                        WHERE uys2.evaluation_year = %s 
                        AND u2.organization_id = u.organization_id
                        AND uys2.final_score > uys.final_score
                        AND uys2.final_score IS NOT NULL
                    )
                    WHERE uys.evaluation_year = %s AND uys.final_score IS NOT NULL
                """, (year, year))
                
                # 랭킹 업데이트 결과 확인
                cur.execute("""
                    SELECT COUNT(*) as total_users,
                           COUNT(CASE WHEN user_rank IS NOT NULL THEN 1 END) as ranked_users,
                           COUNT(CASE WHEN team_rank IS NOT NULL THEN 1 END) as team_ranked_users
                    FROM user_year_scores 
                    WHERE evaluation_year = %s
                """, (year,))
                result = cur.fetchone()
                
                print(f"✅ {year}년 랭킹 계산 완료:")
                print(f"   - 전체 사용자: {result['total_users']}명")
                print(f"   - 전체 랭킹 계산: {result['ranked_users']}명")
                print(f"   - 팀 랭킹 계산: {result['team_ranked_users']}명")
                
                return True
                
        except Exception as e:
            print(f"❌ 랭킹 계산 실패: {e}")
            return False
        finally:
            if 'conn' in locals():
                conn.close()
    
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
            
            # 디버그: 저장하려는 데이터 확인
            final_score = report_data.get("final_score_info", {}).get("overall_final_score", "없음")
            print(f"    💾 MongoDB 저장 시도: User {user_id}, Final Score: {final_score}")
            
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
                
                # MariaDB user_year_scores 테이블에 최종 점수 저장
                mariadb_success = False
                if save_success:
                    # final_score_info에서 최종 점수 추출
                    final_score_info = annual_report.get("final_score_info", {})
                    final_score = final_score_info.get("overall_final_score", 0.0)
                    available_categories = final_score_info.get("available_categories", [])
                    
                    if final_score > 0:
                        mariadb_success = self.save_final_score_to_mariadb(user_id, year, final_score)
                    else:
                        print(f"⚠️  User {user_id}: 최종 점수가 0이므로 MariaDB 저장 건너뜀")
                        mariadb_success = True  # 처리 성공으로 간주
                
                if save_success and mariadb_success:
                    results.append({
                        "success": True,
                        "user_id": user_id,
                        "message": "연간 평가 리포트 생성 및 저장 완료"
                    })
                    # 저장된 리포트에서 다시 값 추출해서 로그 출력
                    final_score_info = annual_report.get("final_score_info", {})
                    final_score = final_score_info.get("overall_final_score", 0.0)
                    available_cats = final_score_info.get("available_categories", [])
                    user_name = annual_report.get("user", {}).get("name", f"User {user_id}")
                    print(f"✓ {user_name}: 연간 평가 완료 (점수: {final_score}, 카테고리: {available_cats})")
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
    print("🎯 연말 평가 에이전트 시작 (논리적 일관성 + 사용자 이름 포함)")
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
    
    # 🔥 중요 사용자 ID 직접 지정 (65, 76, 91번)
    target_user_ids = [65, 76, 91]
    
    print(f"\n🎯 {evaluation_year}년 연간 평가 대상자 (중요 사용자):")
    print(f"✅ 평가 대상자: {target_user_ids} (총 {len(target_user_ids)}명)")
    
    # 대상 사용자 유효성 검사
    print(f"\n🔍 대상 사용자 유효성 검사 중...")
    valid_user_ids = []
    for user_id in target_user_ids:
        user_info = agent.get_user_basic_info(user_id)
        if user_info["name"] != f"직원 {user_id}번":  # 실제 사용자 데이터가 있는지 확인
            valid_user_ids.append(user_id)
            print(f"✅ User {user_id}: {user_info['name']} ({user_info['job_name']})")
        else:
            print(f"⚠️  User {user_id}: 사용자 정보 없음, 평가 진행")
            valid_user_ids.append(user_id)  # 데이터가 없어도 평가 시도
    
    if not valid_user_ids:
        print("❌ 유효한 사용자를 찾을 수 없습니다. 프로그램을 종료합니다.")
        agent.close()
        return
    
    print(f"✅ 최종 평가 대상자: {valid_user_ids} (총 {len(valid_user_ids)}명)")
    
    # 연간 평가 배치 처리 (중요 사용자 대상)
    results = agent.process_annual_evaluations(valid_user_ids, evaluation_year)
    
    # 중요 사용자 처리 완료 후 랭킹 계산 (전체 사용자 대상)
    print(f"\n🏆 {evaluation_year}년 전체 랭킹 계산 시작...")
    ranking_success = agent.calculate_and_update_rankings(evaluation_year)
    
    # 결과 통계
    successful_count = sum(1 for r in results if r["success"])
    failed_count = len(results) - successful_count
    
    print(f"\n🎉 {evaluation_year}년 중요 사용자 연간 평가 완료!")
    print("=" * 60)
    print(f"대상 사용자: {target_user_ids}")
    print(f"성공: {successful_count}명")
    print(f"실패: {failed_count}명")
    print(f"저장 위치: {MONGO_CONFIG['db_name']}.final_score_results")
    print(f"저장 구조: {evaluation_year}년 연간 문서 → users.{{user_id}} 형태")
    print(f"프롬프트 개선: 논리적 일관성 보장 + 사용자 이름 포함 (Temperature 0)")
    
    # 개선사항 요약
    print(f"\n📊 최종 개선된 annual_comment_summaries:")
    print(f"1. 정량평가: 성과 패턴과 구체적 성과의 논리적 일관성 보장")
    print(f"2. 정성평가: 실제 언급된 행동 특성만 (추측 금지)")
    print(f"3. 동료평가: 실제 동료 평가 내용으로 자연스럽게 종료")
    print(f"4. 사용자 이름: 모든 문장에 정확한 사용자 이름 포함")
    print(f"5. Temperature 0: 일관되고 안정적인 결과 생성")
    
    # 중요 사용자 결과 상세 출력
    print(f"\n🎯 중요 사용자 평가 결과:")
    for result in results:
        user_id = result["user_id"]
        status = "✅ 성공" if result["success"] else "❌ 실패"
        print(f"   User {user_id}: {status} - {result['message']}")
    
    # 백업 파일 저장
    backup_filename = f"annual_evaluation_results_{evaluation_year}_final.json"
    backup_data = {
        "year": evaluation_year,
        "target_users": target_user_ids,
        "valid_users": valid_user_ids,
        "total_users": len(valid_user_ids),
        "successful_count": successful_count,
        "failed_count": failed_count,
        "improvement_notes": {
            "prompt_enhancement": "논리적 일관성 보장 + 사용자 이름 포함 (Temperature 0)",
            "quantitative": "성과 패턴과 구체적 성과의 논리적 일관성 보장",
            "qualitative": "실제 언급된 행동 특성만 (추측 금지)",
            "peer": "실제 동료 평가 내용으로 자연스럽게 종료",
            "user_name": "모든 문장에 정확한 사용자 이름 포함",
            "temperature": "0 (일관된 결과 생성)"
        },
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