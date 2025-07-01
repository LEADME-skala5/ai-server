import pymysql
import os
from decimal import Decimal
from dotenv import load_dotenv

# .env 불러오기
load_dotenv()

# 공통 DB 연결 함수
def get_connection():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_DATABASE'),
        port=int(os.getenv('DB_PORT', 3306)),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor 
    )

# Decimal을 float로 안전하게 변환하는 헬퍼 함수
def safe_float(value) -> float:
    """Decimal, None, 기타 타입을 안전하게 float로 변환"""
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

### 1. 평균 평가 점수 계산
def get_average_grade(evaluatee_user_id: int, year: int, quarter: int) -> float:
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT AVG(grade) as avg_grade
        FROM weekly_evaluations
        WHERE evaluatee_user_id = %s
          AND evaluation_year = %s
          AND evaluation_quarter = %s
    """
    cursor.execute(query, (evaluatee_user_id, year, quarter))
    result = cursor.fetchone()

    conn.close()
    
    # DictCursor 사용 시 딕셔너리로 접근
    avg_value = safe_float(result['avg_grade'] if result else 0)
    return round(avg_value, 2)

### 2. 업무량 점수 (가중 평균 계산)
def get_weighted_workload_score(evaluatee_user_id: int, year: int, quarter: int) -> float:
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT we.grade, t.weight
        FROM weekly_evaluations we
        JOIN tasks t ON we.task_id = t.id
        WHERE we.evaluatee_user_id = %s
          AND we.evaluation_year = %s
          AND we.evaluation_quarter = %s
    """
    cursor.execute(query, (evaluatee_user_id, year, quarter))
    rows = cursor.fetchall()

    conn.close()

    if not rows:
        return 0.0

    # DictCursor 사용 시 딕셔너리로 접근
    numerator = sum(safe_float(row['grade']) * safe_float(row['weight']) for row in rows)
    denominator = sum(safe_float(row['weight']) for row in rows)

    return round(numerator / denominator, 2) if denominator > 0 else 0.0


### 3. 최종 점수 계산
def calculate_final_score(avg_score: float, workload_score: float) -> float:
    # 입력값들도 안전하게 float로 변환
    avg_safe = safe_float(avg_score)
    workload_safe = safe_float(workload_score)
    
    final_score = avg_safe * 0.7 + workload_safe * 0.3
    return round(final_score, 2)

### 4. 테스트 실행 예시
if __name__ == "__main__":
    evaluatee_id = 82
    year = 2024
    quarter = 4

    avg = get_average_grade(evaluatee_id, year, quarter)
    workload = get_weighted_workload_score(evaluatee_id, year, quarter)
    final = calculate_final_score(avg, workload)

    print(f"[팀원 {evaluatee_id}] {year}년 {quarter}분기 평가")
    print(f"- 평균 점수: {avg}")
    print(f"- 업무량 점수: {workload}")
    print(f"- 최종 점수: {final}")
    print(f"- 타입 확인: avg={type(avg)}, workload={type(workload)}, final={type(final)}")