import os
import pymysql
import json
from dotenv import load_dotenv
from pathlib import Path
from pymongo import MongoClient
from datetime import datetime

# 환경변수 로드
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# .env에서 DB 설정 불러오기
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
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

class MongoDBManager:
    """MongoDB 연결 및 관리 클래스"""
    
    def __init__(self):
        self.mongodb_uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        self.database_name = MONGO_CONFIG["db_name"]
        self.collection_name = "final_score_results"  # ✅ 변경: personal_quarter_reports → final_score_results
        self.client = None
    
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
    
    def save_final_score_data(self, quarter_data: dict) -> bool:
        """최종 점수 데이터를 MongoDB에 저장"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db[self.collection_name]
            
            # 평가 기간에서 연도와 분기 추출
            evaluation_period = quarter_data.get("meta", {}).get("evaluation_period", "")
            if evaluation_period:
                # "2024Q1" 형식에서 연도와 분기 추출
                year = int(evaluation_period[:4])
                quarter = int(evaluation_period[5:])
            else:
                year = 2024
                quarter = 1
            
            # ✅ 새로운 구조: final_score로 감싸지 않고 직접 저장
            final_score_document = {
                "type": "final-score-quarter",           # 타입 구분
                "evaluated_year": year,                  # 평가 연도
                "evaluated_quarter": quarter,            # 평가 분기
                "meta": quarter_data.get("meta", {}),    # 메타 정보
                "statistics": quarter_data.get("statistics", {}),  # 통계 정보
                "evaluations": quarter_data.get("evaluations", []), # 평가 결과 배열
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
            
            result = collection.insert_one(final_score_document)
            print(f"✅ MongoDB 저장 완료 - Document ID: {result.inserted_id}")
            print(f"   타입: final-score-quarter")
            print(f"   연도/분기: {year}년 {quarter}분기")
            print(f"   컬렉션: {self.database_name}.{self.collection_name}")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 저장 실패: {e}")
            return False
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()
            print("MongoDB 연결 종료")

# 평가 년도 및 분기
EVAL_YEAR = 2024
EVAL_QUARTER = 1

# JSON 파일 업데이트 함수
def update_json_file_with_final_score(user_id, eval_year, eval_quarter, final_score, output_dir="output"):
    filename = f"{output_dir}/evaluation_user_{user_id}_{eval_year}Q{eval_quarter}.json"
    
    try:
        # 기존 JSON 파일 읽기
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # final_score를 최상위 레벨에 추가
            data["finalScore"] = final_score
            
            # 파일 다시 저장
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"📄 JSON 파일 업데이트 완료: {filename}")
        else:
            print(f"⚠️ JSON 파일이 존재하지 않습니다: {filename}")
    except Exception as e:
        print(f"❌ JSON 파일 업데이트 오류: {e}")

def get_user_scores_from_db(user_id, eval_year, eval_quarter, cursor):
    """user_quarter_scores 테이블에서 사용자의 모든 점수 조회"""
    cursor.execute("""
        SELECT 
            weekly_score,
            qualitative_score,
            peer_score
        FROM user_quarter_scores 
        WHERE user_id = %s AND evaluation_year = %s AND evaluation_quarter = %s
    """, (user_id, eval_year, eval_quarter))
    
    result = cursor.fetchone()
    if not result:
        return None, None, None
    
    # None 값을 0.0으로 처리하고 float 변환
    weekly_score = float(result['weekly_score']) if result['weekly_score'] is not None else 0.0
    qualitative_score = float(result['qualitative_score']) if result['qualitative_score'] is not None else 0.0
    peer_score = float(result['peer_score']) if result['peer_score'] is not None else 0.0
    
    return weekly_score, qualitative_score, peer_score

def process_single_quarter_final_scores(mongodb_manager, eval_year, eval_quarter):
    """단일 분기 최종 점수 처리"""
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # 1. user_quarter_scores 테이블에서 해당 분기 데이터가 있는 사용자 조회
            cur.execute("""
                SELECT DISTINCT user_id 
                FROM user_quarter_scores 
                WHERE evaluation_year = %s AND evaluation_quarter = %s
            """, (eval_year, eval_quarter))
            
            users = cur.fetchall()
            if not users:
                print(f"❌ {eval_year}년 {eval_quarter}분기 user_quarter_scores 데이터가 없습니다.")
                return None

            print(f"🎯 처리 대상: {len(users)}명 ({eval_year}년 {eval_quarter}분기)")
            print("=" * 60)

            successful_count = 0
            failed_count = 0
            results = []

            for user in users:
                user_id = user['user_id']

                # 2. DB에서 모든 점수 조회
                weekly_score, qualitative_score, peer_score = get_user_scores_from_db(
                    user_id, eval_year, eval_quarter, cur
                )

                if weekly_score is None:
                    print(f"⚠️ user_id={user_id}: 점수 데이터 없음")
                    results.append({
                        "success": False,
                        "message": "점수 데이터가 없습니다.",
                        "data": None
                    })
                    failed_count += 1
                    continue

                # 3. 점수 유효성 검증
                missing_scores = []
                if weekly_score == 0.0:
                    missing_scores.append("weekly_score")
                if qualitative_score == 0.0:
                    missing_scores.append("qualitative_score")
                if peer_score == 0.0:
                    missing_scores.append("peer_score")

                if missing_scores:
                    print(f"⚠️ user_id={user_id}: 누락된 점수 {missing_scores}. 사용 가능한 점수로 계산 진행.")

                # 디버깅 출력
                print(f"------[점수 계산] user_id: {user_id}------")
                print(f"정량 평가 점수 (weekly_score): {weekly_score}")
                print(f"정성 평가 점수 (qualitative_score): {qualitative_score}")
                print(f"동료 평가 점수 (peer_score): {peer_score}")

                # 4. 가중 평균 계산 (40% : 30% : 30%)
                final_score = round(
                    0.4 * weekly_score + 0.3 * qualitative_score + 0.3 * peer_score,
                    2
                )
                print(f"계산된 최종 점수: {final_score}")

                # 5. DB 저장 - final_score 컬럼 업데이트
                cur.execute("""
                    UPDATE user_quarter_scores 
                    SET final_score = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = %s AND evaluation_year = %s AND evaluation_quarter = %s
                """, (final_score, user_id, eval_year, eval_quarter))
                
                if cur.rowcount > 0:
                    print(f"✅ 최종 점수 업데이트 완료: user_id={user_id}, final_score={final_score}")
                    successful_count += 1
                    
                    # 6. JSON 파일에 final_score 추가
                    update_json_file_with_final_score(user_id, eval_year, eval_quarter, final_score)
                    
                    # 결과 추가
                    results.append({
                        "success": True,
                        "data": {
                            "user_id": user_id,
                            "year": eval_year,
                            "quarter": eval_quarter,
                            "final_score": final_score,
                            "scores": {
                                "weekly_score": weekly_score,
                                "qualitative_score": qualitative_score,
                                "peer_score": peer_score
                            },
                            "calculation_method": "weighted_average_40_30_30"
                        }
                    })
                else:
                    print(f"❌ user_id={user_id} 업데이트 실패")
                    failed_count += 1
                    results.append({
                        "success": False,
                        "message": "DB 업데이트 실패",
                        "data": None
                    })
                
                print("-" * 40)

            # 통계 계산
            avg_score = None
            if successful_count > 0:
                # 통계 조회
                cur.execute("""
                    SELECT 
                        AVG(final_score) as avg_score,
                        MAX(final_score) as max_score,
                        MIN(final_score) as min_score
                    FROM user_quarter_scores 
                    WHERE evaluation_year = %s AND evaluation_quarter = %s 
                    AND final_score IS NOT NULL
                """, (eval_year, eval_quarter))
                
                stats = cur.fetchone()
                if stats:
                    avg_score = float(stats['avg_score'])
                    max_score = float(stats['max_score'])
                    min_score = float(stats['min_score'])
                    
                    print(f"📈 통계:")
                    print(f"   평균 점수: {avg_score:.2f}")
                    print(f"   최고 점수: {max_score:.2f}")
                    print(f"   최저 점수: {min_score:.2f}")

            # 결과를 구조화된 형태로 변환
            formatted_results = {
                "meta": {
                    "evaluation_period": f"{eval_year}Q{eval_quarter}",
                    "total_users_processed": len(results),
                    "successful_evaluations": successful_count,
                    "failed_evaluations": failed_count,
                    "processing_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "version": "v1",
                    "calculation_method": "weighted_average_40_30_30"
                },
                "statistics": {
                    "average_score": round(avg_score, 2) if avg_score else None,
                    "max_score": round(max_score, 2) if successful_count > 0 else None,
                    "min_score": round(min_score, 2) if successful_count > 0 else None
                },
                "evaluations": results
            }

            # 최종 결과 출력
            print("=" * 60)
            print(f"🎉 최종 점수 계산 완료!")
            print(f"✅ 성공: {successful_count}명")
            print(f"❌ 실패: {failed_count}명")
            print(f"📊 가중치: 정량(40%) + 정성(30%) + 동료평가(30%)")
            
            return formatted_results

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()

def check_score_availability():
    """각 점수 유형별 데이터 현황 체크"""
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            print(f"📊 {EVAL_YEAR}년 {EVAL_QUARTER}분기 점수 데이터 현황:")
            print("=" * 50)
            
            # 전체 사용자 수
            cur.execute("""
                SELECT COUNT(*) as total_users
                FROM user_quarter_scores 
                WHERE evaluation_year = %s AND evaluation_quarter = %s
            """, (EVAL_YEAR, EVAL_QUARTER))
            total_users = cur.fetchone()['total_users']
            print(f"총 사용자 수: {total_users}명")
            
            # 각 점수별 데이터 현황
            score_types = [
                ('weekly_score', '정량 평가'),
                ('qualitative_score', '정성 평가'), 
                ('peer_score', '동료 평가')
            ]
            
            for column, description in score_types:
                cur.execute(f"""
                    SELECT 
                        COUNT(*) as total,
                        COUNT({column}) as has_score,
                        COUNT(*) - COUNT({column}) as missing
                    FROM user_quarter_scores 
                    WHERE evaluation_year = %s AND evaluation_quarter = %s
                """, (EVAL_YEAR, EVAL_QUARTER))
                
                result = cur.fetchone()
                has_score = result['has_score']
                missing = result['missing']
                coverage = (has_score / total_users * 100) if total_users > 0 else 0
                
                print(f"{description}: {has_score}명 ({coverage:.1f}%) | 누락: {missing}명")
            
            print("=" * 50)
            
    except Exception as e:
        print(f"❌ 현황 체크 오류: {e}")
    finally:
        conn.close()

def main():
    print("🚀 최종 점수 계산 시작")
    print("=" * 60)
    
    # MongoDB 매니저 초기화
    mongodb_manager = MongoDBManager()
    if not mongodb_manager.connect():
        print("❌ MongoDB 연결 실패. 로컬 저장만 진행합니다.")
    
    # 전체 결과 저장용
    all_quarters_results = {}
    
    # 4개 분기 모두 처리
    for quarter in [1, 2, 3, 4]:
        print(f"\n=== {EVAL_YEAR}년 {quarter}분기 최종 점수 계산 ===")
        
        # 1. 현재 데이터 현황 체크
        print(f"📊 {EVAL_YEAR}년 {quarter}분기 점수 데이터 현황:")
        conn = pymysql.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) as total_users
                    FROM user_quarter_scores 
                    WHERE evaluation_year = %s AND evaluation_quarter = %s
                """, (EVAL_YEAR, quarter))
                total_users = cur.fetchone()['total_users']
                print(f"총 사용자 수: {total_users}명")
        finally:
            conn.close()
        
        if total_users == 0:
            print(f"⚠️ {quarter}분기 데이터 없음. 다음 분기로 넘어갑니다.")
            continue
        
        # 2. 최종 점수 계산 및 저장
        quarter_result = process_single_quarter_final_scores(mongodb_manager, EVAL_YEAR, quarter)
        
        if quarter_result:
            all_quarters_results[f"Q{quarter}"] = quarter_result
            
            # MongoDB에 저장
            print(f"\n📦 MongoDB에 저장 중...")
            success = mongodb_manager.save_final_score_data(quarter_result)
            
            if success:
                print(f"✅ {EVAL_YEAR}Q{quarter} 데이터가 MongoDB에 성공적으로 저장되었습니다!")
            else:
                print(f"❌ MongoDB 저장 실패. JSON 파일로 백업 저장합니다.")
                backup_filename = f"final_score_results_{EVAL_YEAR}Q{quarter}_backup.json"
                with open(backup_filename, 'w', encoding='utf-8') as f:
                    json.dump(quarter_result, f, ensure_ascii=False, indent=2)
                print(f"백업 파일: {backup_filename}")
        
        print("\n" + "=" * 60)
    
    # 전체 분기 통합 결과 출력
    print(f"\n=== {EVAL_YEAR}년 전체 분기 최종 점수 계산 완료 ===")
    
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["meta"]["successful_evaluations"]
            print(f"Q{quarter}: 성공 {successful}명 → type: 'final-score-quarter', evaluated_year: {EVAL_YEAR}, evaluated_quarter: {quarter}")
        else:
            print(f"Q{quarter}: 데이터 없음")
    
    print(f"\n🎉 처리 완료 요약:")
    print(f"  - 저장 방식: final_score_results 컬렉션에 type별로 구분")
    print(f"  - 데이터베이스: {MONGO_CONFIG['db_name']}")
    print(f"  - 컬렉션: final_score_results")
    print(f"  - 문서 개수: {len(all_quarters_results)}개 (각 분기별)")
    print(f"  - 문서 구조: type/evaluated_year/evaluated_quarter/meta/statistics/evaluations[]")
    print(f"  - 계산 방식: 가중 평균 (정량 40% + 정성 30% + 동료평가 30%)")
    
    # MongoDB 연결 종료
    mongodb_manager.close()

if __name__ == '__main__':
    main()