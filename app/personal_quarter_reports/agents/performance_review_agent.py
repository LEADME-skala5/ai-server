import os
import json
import pymysql
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
from pathlib import Path
from pymongo import MongoClient
from openai import OpenAI
import logging

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

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB 설정
MONGO_CONFIG = {
    "host": os.getenv("MONGO_HOST"),
    "port": int(os.getenv("MONGO_PORT")),
    "username": os.getenv("MONGO_USER"),
    "password": os.getenv("MONGO_PASSWORD"),
    "db_name": os.getenv("MONGO_DB_NAME")
}

class MongoDBManager:
    """MongoDB 연결 및 관리 클래스"""
    
    def __init__(self):
        self.mongodb_uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        self.database_name = MONGO_CONFIG["db_name"]
        self.output_collection_name = "final_performance_reviews"  # 출력 컬렉션
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
    
    def get_user_data_from_collection(self, collection_name: str, user_id: int, year: int, quarter: int) -> Optional[Dict]:
        """특정 컬렉션에서 사용자 데이터 조회"""
        try:
            if not self.client:
                if not self.connect():
                    return None
            
            db = self.client[self.database_name]
            collection = db[collection_name]
            
            # type: "personal-quarter", evaluated_year, evaluated_quarter로 문서 조회
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
            print(f"❌ MongoDB 데이터 조회 실패 (collection: {collection_name}, user: {user_id}): {e}")
            return None
    
    def add_user_to_quarter_document(self, user_data: Dict) -> bool:
        """분기별 문서에 사용자 데이터 추가 - 새로운 형식"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db[self.output_collection_name]
            
            # 해당 분기 문서가 존재하는지 확인
            existing_doc = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": user_data['year'],
                "evaluated_quarter": user_data['quarter']
            })
            
            if existing_doc:
                # 기존 문서에 사용자 데이터 추가
                collection.update_one(
                    {
                        "type": "personal-quarter",
                        "evaluated_year": user_data['year'],
                        "evaluated_quarter": user_data['quarter']
                    },
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
                    "type": "personal-quarter",
                    "evaluated_year": user_data['year'],
                    "evaluated_quarter": user_data['quarter'],
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

class PerformanceReviewAgent:
    """MongoDB 기반 성과 검토 및 최종 평가 에이전트"""
    
    def __init__(self):
        # OpenAI API 키 설정
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API 키를 찾을 수 없습니다. .env 파일에 OPENAI_API_KEY를 설정하세요.")
        
        masked_key = api_key[:10] + "*" * (len(api_key) - 15) + api_key[-5:]
        print(f"🔑 OpenAI API 키 로드 완료: {masked_key}")
        self.client = OpenAI(api_key=api_key)
        
        # MongoDB 매니저 초기화
        self.mongodb_manager = MongoDBManager()
    
    def get_user_info(self, user_id: int) -> Dict:
        """users 테이블에서 사용자 기본 정보 조회"""
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
            logger.warning(f"사용자 정보 조회 실패 (user_id: {user_id}): {e}")
        finally:
            if 'conn' in locals():
                conn.close()
        
        return {
            "name": f"직원 {user_id}번",
            "job_name": "미지정",
            "job_years": 0,
            "organization_id": None
        }
    
    def get_peer_evaluation_data(self, user_id: int, year: int, quarter: int) -> Optional[Dict]:
        """동료평가 데이터 조회 - peer_evaluation_results 컬렉션"""
        return self.mongodb_manager.get_user_data_from_collection("peer_evaluation_results", user_id, year, quarter)
    
    def get_qualitative_evaluation_data(self, user_id: int, year: int, quarter: int) -> Optional[Dict]:
        """정성평가 데이터 조회 - qualitative_evaluation_results 컬렉션"""
        return self.mongodb_manager.get_user_data_from_collection("qualitative_evaluation_results", user_id, year, quarter)
    
    def get_weekly_evaluation_data(self, user_id: int, year: int, quarter: int) -> Optional[Dict]:
        """주간평가 데이터 조회 - weekly_evaluation_results 컬렉션"""
        return self.mongodb_manager.get_user_data_from_collection("weekly_evaluation_results", user_id, year, quarter)
    
    def get_ranking_data(self, user_id: int, year: int, quarter: int) -> Optional[Dict]:
        """랭킹 데이터 조회 - ranking_results 컬렉션"""
        return self.mongodb_manager.get_user_data_from_collection("ranking_results", user_id, year, quarter)
    
    def collect_all_evaluation_data(self, user_id: int, year: int, quarter: int) -> Dict:
        """모든 평가 데이터 수집"""
        print(f"🔍 사용자 ID {user_id}의 {year}Q{quarter} 평가 데이터 수집 중...")
        
        # 각 평가 모듈의 결과 조회 (각기 다른 컬렉션에서)
        peer_data = self.get_peer_evaluation_data(user_id, year, quarter)
        qualitative_data = self.get_qualitative_evaluation_data(user_id, year, quarter)
        weekly_data = self.get_weekly_evaluation_data(user_id, year, quarter)
        ranking_data = self.get_ranking_data(user_id, year, quarter)
        
        # 데이터 존재 여부 확인
        data_status = {
            "peer_evaluation": "✅" if peer_data else "❌",
            "qualitative_evaluation": "✅" if qualitative_data else "❌",
            "weekly_evaluation": "✅" if weekly_data else "❌",
            "ranking": "✅" if ranking_data else "❌"
        }
        
        print(f"📊 데이터 수집 상태: {data_status}")
        
        return {
            "peer_evaluation": peer_data,
            "qualitative_evaluation": qualitative_data,
            "weekly_evaluation": weekly_data,
            "ranking": ranking_data,
            "collection_status": data_status
        }
    
    def generate_activity_summary(self, all_data: Dict) -> str:
        """활동 요약 생성"""
        activities = []
        total_count = 0
        
        # 주간평가 데이터에서 활동 추출
        weekly_data = all_data.get("weekly_evaluation", {})
        if weekly_data:
            team_goals = weekly_data.get("teamGoals", [])
            key_achievements = weekly_data.get("keyAchievements", [])
            
            if team_goals:
                goal_count = len([g for g in team_goals if g.get('assigned') == '배정'])
                if goal_count > 0:
                    activities.append(f"- [팀 목표 참여] {goal_count}건: 팀 목표 달성에 기여 ▶ 평가: 양호")
                    total_count += goal_count
            
            if key_achievements:
                achievement_count = len(key_achievements)
                activities.append(f"- [주요 성과] {achievement_count}건: {', '.join(key_achievements[:2])} 등 ▶ 평가: 우수")
                total_count += achievement_count
        
        # 동료평가에서 활동 유추
        peer_data = all_data.get("peer_evaluation", {})
        if peer_data:
            keyword_summary = peer_data.get("keyword_summary", {})
            positive_keywords = keyword_summary.get('positive', [])
            
            if positive_keywords:
                activities.append(f"- [협업 활동] 다수건: {', '.join(positive_keywords[:3])} 관련 업무 ▶ 평가: 우수")
                total_count += 3
        
        if not activities:
            activities.append("- [일반 업무] 정기 업무 수행 ▶ 평가: 보통")
            total_count = 1
        
        return '\n'.join(activities), total_count
    
    def extract_pattern_analysis(self, all_data: Dict) -> Dict:
        """정성 패턴 분석"""
        strengths = []
        weaknesses = []
        work_style = "일반적"
        
        # 동료평가에서 패턴 추출
        peer_data = all_data.get("peer_evaluation", {})
        if peer_data:
            keyword_summary = peer_data.get("keyword_summary", {})
            strengths.extend(keyword_summary.get('positive', [])[:3])
            weaknesses.extend(keyword_summary.get('negative', [])[:2])
        
        # 정성평가에서 패턴 추출
        qualitative_data = all_data.get("qualitative_evaluation", {})
        if qualitative_data:
            work_attitude = qualitative_data.get("work_attitude", [])
            if work_attitude:
                work_style = f"{', '.join(work_attitude[:2])} 중심"
        
        # 기본값 설정
        if not strengths:
            strengths = ["성실성", "책임감"]
        if not weaknesses:
            weaknesses = ["소통", "효율성"]
        
        return {
            "strengths": ', '.join(strengths),
            "weaknesses": ', '.join(weaknesses),
            "work_style": work_style
        }
    
    def generate_new_format_prompt(self, user_id: int, year: int, quarter: int, all_data: Dict) -> str:
        """새로운 형식의 프롬프트 생성"""
        
        # 사용자 정보 조회
        user_info = self.get_user_info(user_id)
        name = user_info["name"]
        job_name = user_info["job_name"]
        job_years = user_info["job_years"]
        
        # 데이터 추출
        peer_data = all_data.get("peer_evaluation", {})
        qualitative_data = all_data.get("qualitative_evaluation", {})
        weekly_data = all_data.get("weekly_evaluation", {})
        ranking_data = all_data.get("ranking", {})
        
        # 활동 요약 생성
        activity_summary, total_activities = self.generate_activity_summary(all_data)
        
        # 패턴 분석
        pattern_analysis = self.extract_pattern_analysis(all_data)
        
        # 데이터 값 추출 및 기본값 설정
        peer_score = peer_data.get("peer_evaluation_score", 0) if peer_data else 0
        peer_keywords_pos = ', '.join(peer_data.get("keyword_summary", {}).get('positive', ["협업"])) if peer_data else "협업"
        peer_keywords_neg = ', '.join(peer_data.get("keyword_summary", {}).get('negative', ["소통"])) if peer_data else "소통"
        peer_feedback_summary = peer_data.get("feedback", "동료들과 원활한 협업을 보여줌") if peer_data else "동료들과 원활한 협업을 보여줌"
        
        qualitative_score = qualitative_data.get("qualitative_score", 0) if qualitative_data else 0
        attitude_summary = ', '.join(qualitative_data.get("work_attitude", ["성실함"])) if qualitative_data else "성실함"
        
        weekly_score = weekly_data.get("evaluationScore", {}).get("weeklyScore", 0) if weekly_data else 0
        num_team_goals = len([g for g in weekly_data.get("teamGoals", []) if g.get('assigned') == '배정']) if weekly_data else 0
        key_achievements = ', '.join(weekly_data.get("keyAchievements", ["정기 업무 수행"])[:3]) if weekly_data else "정기 업무 수행"
        
        if ranking_data:
            ranking_info = ranking_data.get("ranking_info", {})
            scores = ranking_data.get("scores", {})
            rank_in_jobgroup = ranking_info.get('rank', 'N/A')
            total_in_jobgroup = ranking_info.get('total_in_group', 'N/A')
            rank_in_team = ranking_info.get('team_rank', 'N/A')
            total_in_team = ranking_info.get('total_in_team', 'N/A')
            final_score = scores.get('final_score', 0)
        else:
            rank_in_jobgroup = total_in_jobgroup = rank_in_team = total_in_team = 'N/A'
            final_score = 0
        
        prompt = f"""
당신은 기업 인사팀의 성과 분석 전문가이며, 작성한 결과물은 인사 평가 문서에 직접 활용됩니다.  
다음은 직원 {name}({user_id})의 {year}년 {quarter}분기 활동 데이터입니다.  
이 정보를 바탕으로 아래 항목을 작성하십시오:

1. **분기 성과 종합 요약 (총평)**

---
[입력 데이터 요약]  
- 평가 기간: {year}년 {quarter}분기  
- 직무 및 연차: {job_name}, {job_years}년차  
- 총 활동 수: {total_activities}건  

[주요 업무 활동 및 평가 요약]  
{activity_summary}

[정성 패턴 분석]  
- 강점: {pattern_analysis['strengths']}  
- 개선점: {pattern_analysis['weaknesses']}  
- 업무 스타일: {pattern_analysis['work_style']}

[동료 피드백 요약]  
- 동료평가 점수: {peer_score}/5.0  
- 긍정 키워드: {peer_keywords_pos}  
- 보완 키워드: {peer_keywords_neg}  
- 피드백 요약: {peer_feedback_summary}

[정성 평가 요약]  
- 평가 점수: {qualitative_score}/5.0  
- 업무 태도 평가: {attitude_summary}

[주간 평가]  
- 점수: {weekly_score}/5.0  
- 참여 팀 목표 수: {num_team_goals}개  
- 주요 성과: {key_achievements}

[랭킹]  
- 직군 내 순위: {rank_in_jobgroup}/{total_in_jobgroup}  
- 팀 내 순위: {rank_in_team}/{total_in_team}  
- 최종 점수: {final_score}/5.0

---
[작성 지침]  

**분기 성과 종합 요약 (`performance_summary`)**  
총 **5문장**으로 구성하며,
다음 문장 구조를 그대로 따르십시오 (내용만 바꿔 채워 넣으십시오):

① {name}님은 이번 분기 동안 {job_name} 직무에서 총 {total_activities}건의 활동을 수행하며 {{핵심 업무 요약 또는 기여 내용}}을 중심으로 성과를 도출하였습니다.  
② {{핵심성과 항목}}에서 상위 {{퍼센트}}% 수준을 기록하였으며, 이는 {name} {{핵심 해석}}  
③ 동료들은 {name}님의 {{열거된 역량 키워드}}를 높이 평가했습니다.  
④ 이번 분기의 {name}님은 {{핵심 역량/태도 요약}} 측면에서 뚜렷한 특징을 보였습니다.  
⑤ 개선이 필요한 업무 항목으로는 {{업무 개선 항목 나열}} 등이 있으며, 동료 피드백에서는 {{보완 키워드 요약}}이 보완 키워드로 제시되었습니다.

※ 문체 지침:
- 어미는 단정형(~하였습니다, ~보였습니다)  
- 모호한 표현(예: 양호한 수준, 무난한 편 등)은 금지  
- 순위, 점수는 문장 내 반복하지 마십시오  

**출력 형식은 반드시 아래 JSON 형태를 따르십시오:**  

```json
{{
  "performance_summary": "5문장 고정 구조의 성과 요약 (각 문장은 줄바꿈 포함)"
}}
```"""
        
        return prompt
    
    def generate_ai_review(self, prompt: str) -> Dict:
        """AI 기반 성과 검토 생성"""
        try:
            print("🤖 AI 기반 종합 성과 검토 생성 중...")
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1500,
                temperature=0.3
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # JSON 파싱 시도
            try:
                # 코드 블록 제거 (```json ... ``` 형태)
                if "```json" in result_text and "```" in result_text:
                    start = result_text.find("```json") + 7
                    end = result_text.rfind("```")
                    json_text = result_text[start:end].strip()
                elif "```" in result_text:
                    # 일반 코드 블록 제거
                    start = result_text.find("```") + 3
                    end = result_text.rfind("```")
                    json_text = result_text[start:end].strip()
                else:
                    json_text = result_text
                
                result = json.loads(json_text)
                
                # 결과 검증 및 정리
                if "performance_summary" in result:
                    print("✅ AI 성과 검토 생성 완료")
                    return result
                else:
                    print("⚠️ JSON 구조 불완전, 수동 파싱 시도")
                    raise json.JSONDecodeError("구조 불완전", json_text, 0)
                    
            except json.JSONDecodeError as e:
                print(f"⚠️ JSON 파싱 실패: {str(e)}")
                print("🔧 텍스트에서 수동으로 내용 추출 시도...")
                
                # 텍스트에서 수동으로 파싱
                lines = result_text.split('\n')
                performance_summary = ""
                
                # performance_summary 추출
                in_summary = False
                for line in lines:
                    if '"performance_summary"' in line or 'performance_summary' in line:
                        in_summary = True
                        # 같은 줄에 내용이 있으면 추출
                        if ':' in line:
                            summary_start = line.split(':', 1)[1].strip().strip('"').strip(',')
                            if summary_start:
                                performance_summary = summary_start
                    elif in_summary and line.strip().startswith('"') and not line.strip().endswith('}'):
                        performance_summary += line.strip().strip('"').strip(',') + " "
                    elif in_summary and ('}' in line):
                        break
                
                # 결과 정리
                if not performance_summary:
                    performance_summary = "AI 생성 결과를 파싱할 수 없어 종합 검토를 제공할 수 없습니다."
                
                print(f"✅ 수동 파싱 완료 - 검토문: {len(performance_summary)}자")
                
                return {
                    "performance_summary": performance_summary.strip()
                }
                
        except Exception as e:
            logger.error(f"AI 성과 검토 생성 실패: {str(e)}")
            return {
                "performance_summary": f"성과 검토 생성 중 오류가 발생했습니다: {str(e)}"
            }
    
    def process_user_performance_review(self, user_id: int, year: int, quarter: int, save_to_mongodb: bool = True) -> Dict:
        """개별 사용자 성과 검토 처리"""
        try:
            print(f"\n🎯 사용자 ID {user_id}의 {year}Q{quarter} 성과 검토 생성 시작")
            
            # 1. 모든 평가 데이터 수집
            all_data = self.collect_all_evaluation_data(user_id, year, quarter)
            
            # 2. 최소한의 데이터 확인
            available_data_count = sum(1 for data in [
                all_data["peer_evaluation"],
                all_data["qualitative_evaluation"], 
                all_data["weekly_evaluation"],
                all_data["ranking"]
            ] if data is not None)
            
            if available_data_count == 0:
                return {
                    "success": False,
                    "message": "해당 사용자의 평가 데이터가 없습니다.",
                    "data": None
                }
            
            print(f"📊 {available_data_count}/4개의 평가 데이터 발견")
            
            # 3. 새로운 형식의 AI 프롬프트 생성
            prompt = self.generate_new_format_prompt(user_id, year, quarter, all_data)
            
            # 4. AI 성과 검토 생성
            ai_result = self.generate_ai_review(prompt)
            
            # 5. 결과 데이터 구성
            result_data = {
                "user_id": user_id,
                "year": year,
                "quarter": quarter,
                "performance_summary": ai_result.get("performance_summary", ""),
                "data_sources": all_data["collection_status"],
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # 6. MongoDB 저장 (final_performance_reviews 컬렉션)
            if save_to_mongodb:
                mongodb_save_success = self.mongodb_manager.add_user_to_quarter_document(result_data)
                
                if mongodb_save_success:
                    print(f"✅ 사용자 ID {user_id} 성과 검토 final_performance_reviews 컬렉션에 추가 완료")
                else:
                    print(f"❌ 사용자 ID {user_id} 성과 검토 MongoDB 저장 실패")
            
            return {
                "success": True,
                "data": result_data
            }
            
        except Exception as e:
            logger.error(f"성과 검토 처리 중 오류 발생: {str(e)}")
            return {
                "success": False,
                "message": f"처리 중 오류가 발생했습니다: {str(e)}",
                "data": None
            }
    
    def process_batch_performance_review(self, user_ids: List[int], year: int, quarter: int) -> List[Dict]:
        """배치 성과 검토 처리"""
        results = []
        total_users = len(user_ids)
        
        for i, user_id in enumerate(user_ids, 1):
            if i % 10 == 0 or i == total_users:
                print(f"처리 진행률: {i}/{total_users} ({i/total_users*100:.1f}%)")
            
            result = self.process_user_performance_review(user_id, year, quarter, save_to_mongodb=True)
            results.append(result)
            
            # 성공/실패 여부 및 결과 출력
            if result["success"]:
                print(f"✓ User {user_id}: 성과 검토 생성 완료 → final_performance_reviews 컬렉션에 저장 완료")
                
                # 터미널에서 결과 미리보기 출력
                data = result["data"]
                print(f"\n📋 === 사용자 ID {user_id} 성과 검토 결과 ===")
                print(f"🎯 분기 성과 종합 요약:")
                
                # 성과 요약을 문장별로 분리하여 출력 (줄바꿈 개선)
                summary_text = data["performance_summary"]
                # 문장을 분리하는 방법들
                sentences = []
                
                # 먼저 줄바꿈으로 분리 시도
                if '\n' in summary_text:
                    sentences = [s.strip() for s in summary_text.split('\n') if s.strip()]
                else:
                    # 마침표로 분리 후 정리
                    temp_sentences = summary_text.split('.')
                    for sentence in temp_sentences:
                        sentence = sentence.strip()
                        if sentence and len(sentence) > 10:  # 의미있는 문장만
                            sentences.append(sentence + '.')
                
                # 5문장이 아닌 경우 전체를 하나로 처리
                if len(sentences) != 5:
                    sentences = [summary_text]
                
                # 문장별로 번호를 매겨 출력 (80자 단위로 줄바꿈)
                for i, sentence in enumerate(sentences, 1):
                    if len(sentence) > 80:
                        # 80자 단위로 줄바꿈
                        wrapped_lines = []
                        words = sentence.split(' ')
                        current_line = f"   {i}. "
                        
                        for word in words:
                            if len(current_line + word + ' ') > 80:
                                wrapped_lines.append(current_line.rstrip())
                                current_line = "      " + word + ' '  # 들여쓰기로 계속
                            else:
                                current_line += word + ' '
                        
                        if current_line.strip():
                            wrapped_lines.append(current_line.rstrip())
                        
                        for line in wrapped_lines:
                            print(line)
                    else:
                        print(f"   {i}. {sentence}")
                
                print(f"\n📊 데이터 소스: {data['data_sources']}")
                print(f"⏰ 처리 시간: {data['processed_at']}")
                print("=" * 60)
                
            else:
                print(f"✗ User {user_id}: {result['message']}")
        
        return results

def process_single_quarter_performance_review(agent: PerformanceReviewAgent, user_ids: List[int], year: int, quarter: int):
    """단일 분기 성과 검토 처리"""
    print(f"\n=== {year}년 {quarter}분기 성과 검토 처리 시작 ===")
    print(f"처리할 사용자 수: {len(user_ids)}명")
    print(f"MongoDB 저장 방식: final_performance_reviews 컬렉션에 type: 'personal-quarter'로 구분")
    print("=" * 50)
    
    # 배치 처리 실행
    results = agent.process_batch_performance_review(user_ids, year, quarter)
    
    # 결과 통계
    successful_count = sum(1 for r in results if r["success"])
    failed_count = len(results) - successful_count
    
    print(f"\n=== {quarter}분기 성과 검토 처리 완료 ===")
    print(f"성공: {successful_count}명 → final_performance_reviews 컬렉션에 저장 완료")
    print(f"실패: {failed_count}명")
    
    # 성공한 사용자들의 요약 통계
    if successful_count > 0:
        print(f"\n📈 {quarter}분기 성과 검토 요약:")
        print(f"   - 총 {successful_count}명의 종합 성과 검토 완료")
        print(f"   - 평균 데이터 수집률: {successful_count * 4}/{len(user_ids) * 4} 모듈")
        print(f"   - AI 기반 맞춤형 성과 요약 생성")
    
    if failed_count > 0:
        print(f"⚠️  데이터가 부족하거나 처리 실패한 사용자: {failed_count}명")
    
    return {
        "quarter": quarter,
        "successful_count": successful_count,
        "failed_count": failed_count
    }

def main():
    print("🚀 MongoDB 기반 성과 검토 시스템 시작 (성과 요약만)")
    print("=" * 60)
    
    # 에이전트 초기화
    agent = PerformanceReviewAgent()
    
    # MongoDB 연결 테스트
    print("🔌 MongoDB 연결 테스트...")
    if not agent.mongodb_manager.connect():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    # 평가 년도 설정
    evaluation_year = 2024
    
    print(f"\n=== {evaluation_year}년 전체 분기 성과 검토 배치 처리 시작 ===")
    print(f"입력 데이터 소스: 4개 분리된 컬렉션")
    print(f"  - peer_evaluation_results")
    print(f"  - qualitative_evaluation_results") 
    print(f"  - weekly_evaluation_results")
    print(f"  - ranking_results")
    print(f"저장 위치: MongoDB - {MONGO_CONFIG['db_name']}.final_performance_reviews")
    print(f"저장 방식: type: 'personal-quarter'로 구분")
    print(f"출력 형식:")
    print(f"  - performance_summary: 5문장으로 구성된 성과 요약")
    print("=" * 60)
    
    # 처리할 사용자 ID 리스트 (1~100)
    user_ids = list(range(1, 101))
    
    # 전체 결과 저장용
    all_quarters_results = {}
    
    # 4개 분기 모두 처리
    for quarter in [1, 2, 3, 4]:
        quarter_result = process_single_quarter_performance_review(agent, user_ids, evaluation_year, quarter)
        all_quarters_results[f"Q{quarter}"] = quarter_result
        
        # 백업 파일도 저장
        backup_filename = f"performance_review_final_{evaluation_year}Q{quarter}_backup.json"
        with open(backup_filename, 'w', encoding='utf-8') as f:
            json.dump(quarter_result, f, ensure_ascii=False, indent=2)
        print(f"📄 백업 파일 저장 완료: {backup_filename}")
        
        # 분기 간 구분
        print("\n" + "=" * 60)
    
    # 전체 분기 통합 결과 출력
    print(f"\n🎉 {evaluation_year}년 전체 분기 성과 검토 처리 완료!")
    print("=" * 60)
    
    total_processed = 0
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"]
            total_processed += successful
            print(f"Q{quarter}: 성공 {successful}명 → type: 'personal-quarter', evaluated_year: {evaluation_year}, evaluated_quarter: {quarter}")
        else:
            print(f"Q{quarter}: 데이터 없음")
    
    print(f"\n🎉 처리 완료 요약:")
    print(f"  - 총 처리된 사용자: {total_processed}명")
    print(f"  - 입력: 4개 분리된 컬렉션에서 데이터 수집")
    print(f"  - 출력: final_performance_reviews 컬렉션")
    print(f"  - 출력 형식:")
    print(f"    • performance_summary: 5문장 성과 요약")
    print(f"    • data_sources: 데이터 수집 현황")
    print(f"  - 저장 방식: type: 'personal-quarter'로 구분")
    print(f"  - 데이터베이스: {MONGO_CONFIG['db_name']}")
    print(f"  - 컬렉션: final_performance_reviews")
    print(f"  - 문서 개수: {len(all_quarters_results)}개 (각 분기별)")
    print(f"  - 문서 구조: type/evaluated_year/evaluated_quarter/user_count/users[]")
    print(f"  - AI 모델: GPT-4o")
    
    # 전체 분기별 상세 결과
    print(f"\n📋 분기별 상세 결과:")
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"] 
            failed = quarter_data["failed_count"]
            success_rate = (successful / (successful + failed)) * 100 if (successful + failed) > 0 else 0
            print(f"  📊 Q{quarter}: {successful}명 성공, {failed}명 실패 (성공률: {success_rate:.1f}%)")
        else:
            print(f"  📊 Q{quarter}: 데이터 없음")
    
    # MongoDB 연결 종료
    agent.mongodb_manager.close()
    
    return all_quarters_results

if __name__ == "__main__":
    main()