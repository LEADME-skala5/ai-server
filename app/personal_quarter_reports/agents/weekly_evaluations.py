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
        db=os.getenv('DB_NAME'),
        port=int(os.getenv('DB_PORT', 3306)),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor
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

# 등급을 점수로 변환하는 함수
def grade_to_score(grade: str) -> float:
    """등급('A', 'B', 'C', 'D', 'E')을 점수(5, 4, 3, 2, 1)로 변환"""
    grade_mapping = {
        'A': 5.0,
        'B': 4.0,
        'C': 3.0,
        'D': 2.0,
        'E': 1.0
    }
    return grade_mapping.get(grade, 0.0)

### 1. 평균 평가 점수 계산 (기존)
def get_average_grade(evaluatee_user_id: int, year: int, quarter: int) -> float:
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT AVG(grade)
        FROM weekly_evaluations
        WHERE evaluatee_user_id = %s
          AND evaluation_year = %s
          AND evaluation_quarter = %s
    """
    cursor.execute(query, (evaluatee_user_id, year, quarter))
    result = cursor.fetchone()[0]

    conn.close()
    
    # Decimal 타입 안전 처리
    avg_value = safe_float(result)
    return round(avg_value, 2)

### 2. 업무량 점수 (가중 평균 계산) - 수정된 조인 조건
def get_weighted_workload_score(evaluatee_user_id: int, year: int, quarter: int) -> float:
    """
    team_goal 테이블의 가중치를 사용하여 업무량 점수 계산
    """
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT we.grade, tg.weight
        FROM weekly_evaluations we
        JOIN tasks t ON we.task_id = t.id
        JOIN team_goal tg ON t.id = tg.id
        WHERE we.evaluatee_user_id = %s
          AND we.evaluation_year = %s
          AND we.evaluation_quarter = %s
    """
    cursor.execute(query, (evaluatee_user_id, year, quarter))
    rows = cursor.fetchall()

    conn.close()

    if not rows:
        print(f"⚠️ 사용자 ID {evaluatee_user_id}의 {year}년 {quarter}분기 업무량 데이터가 없습니다.")
        return 0.0

    # Decimal 타입 안전 처리 및 가중평균 계산
    numerator = sum(safe_float(grade) * safe_float(weight) for grade, weight in rows)
    denominator = sum(safe_float(weight) for _, weight in rows)

    if denominator > 0:
        weighted_avg = numerator / denominator
        print(f"  📊 업무량 점수: {len(rows)}개 평가, 가중평균 {weighted_avg:.2f}")
        return round(weighted_avg, 2)
    else:
        print(f"⚠️ 사용자 ID {evaluatee_user_id}의 {year}년 {quarter}분기 가중치 합계가 0입니다.")
        return 0.0

### 3. 개인 실적 점수 계산 - 수정된 조인 조건
def get_personal_performance_score(user_id: int, quarter: int) -> float:
    """
    사용자의 개인 실적 점수를 team_goal 테이블의 가중치로 계산
    
    Args:
        user_id: 사용자 ID
        quarter: 분기 (1, 2, 3, 4)
    
    Returns:
        float: 가중평균된 개인 실적 점수 (1.0~5.0)
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # 분기에 따른 컬럼명 결정
    quarter_column_map = {
        1: 'q1',
        2: 'q2', 
        3: 'q3',
        4: 'q4'
    }
    
    quarter_column = quarter_column_map.get(quarter)
    if not quarter_column:
        print(f"❌ 잘못된 분기: {quarter}. 1~4 범위여야 합니다.")
        conn.close()
        return 0.0

    # team_goal 테이블의 가중치를 사용하여 사용자 실적 조회
    query = f"""
        SELECT tr.{quarter_column}, tg.weight
        FROM task_results tr
        JOIN task_participations tp ON tr.task_id = tp.task_id
        JOIN tasks t ON tr.task_id = t.id
        JOIN team_goal tg ON t.id = tg.id
        WHERE tp.user_id = %s
          AND tr.{quarter_column} IS NOT NULL
    """
    
    try:
        cursor.execute(query, (user_id,))
        rows = cursor.fetchall()
        
        if not rows:
            print(f"⚠️ 사용자 ID {user_id}의 {quarter}분기 실적 데이터가 없습니다.")
            conn.close()
            return 0.0
        
        # 가중평균 계산
        numerator = 0.0
        denominator = 0.0
        
        for grade, weight in rows:
            if grade:  # grade가 None이 아닌 경우만
                score = grade_to_score(grade)
                weight_val = safe_float(weight)
                numerator += score * weight_val
                denominator += weight_val
        
        conn.close()
        
        if denominator > 0:
            weighted_avg = numerator / denominator
            print(f"  📊 사용자 {user_id} {quarter}분기 실적: {len(rows)}개 task, 가중평균 {weighted_avg:.2f}")
            return round(weighted_avg, 2)
        else:
            print(f"⚠️ 사용자 ID {user_id}의 {quarter}분기 가중치 합계가 0입니다.")
            return 0.0
            
    except Exception as e:
        print(f"❌ 개인 실적 점수 계산 오류 (user_id: {user_id}, quarter: {quarter}): {e}")
        conn.close()
        return 0.0

### 4. 기존 점수 계산 (기존)
def calculate_final_score(avg_score: float, workload_score: float) -> float:
    # 입력값들도 안전하게 float로 변환
    avg_safe = safe_float(avg_score)
    workload_safe = safe_float(workload_score)
    
    final_score = avg_safe * 0.7 + workload_safe * 0.3
    return round(final_score, 2)

### 5. 새로운 최종 점수 계산 (기존 점수 + 개인 실적 점수 조합)
def calculate_enhanced_final_score(user_id: int, year: int, quarter: int) -> dict:
    """
    기존 점수와 개인 실적 점수를 2.5:7.5 비율로 조합하여 최종 점수 계산
    
    Args:
        user_id: 사용자 ID
        year: 평가 연도
        quarter: 평가 분기
    
    Returns:
        dict: 각 점수와 최종 조합 점수를 포함한 딕셔너리
    """
    print(f"🔍 향상된 점수 계산 시작: user_id={user_id}, year={year}, quarter={quarter}")
    
    # 1. 기존 점수 계산 (team_goal 가중치 적용)
    avg_score = get_average_grade(user_id, year, quarter)
    workload_score = get_weighted_workload_score(user_id, year, quarter)
    existing_final_score = calculate_final_score(avg_score, workload_score)
    
    print(f"  📈 기존 점수 계산 (team_goal 가중치 적용):")
    print(f"    - 평균 점수: {avg_score}")
    print(f"    - 업무량 점수: {workload_score}")
    print(f"    - 기존 최종 점수: {existing_final_score}")
    
    # 2. 개인 실적 점수 계산 (team_goal 가중치 적용)
    performance_score = get_personal_performance_score(user_id, quarter)
    print(f"  🎯 개인 실적 점수 (team_goal 가중치 적용): {performance_score}")
    
    # 3. 최종 조합 점수 계산 (기존 25% + 실적 75%)
    enhanced_final_score = existing_final_score * 0.25 + performance_score * 0.75
    enhanced_final_score = round(enhanced_final_score, 2)
    
    print(f"  🏆 최종 조합 점수: {existing_final_score} × 0.25 + {performance_score} × 0.75 = {enhanced_final_score}")
    
    return {
        'user_id': user_id,
        'year': year,
        'quarter': quarter,
        'avg_score': avg_score,
        'workload_score': workload_score,
        'existing_final_score': existing_final_score,
        'performance_score': performance_score,
        'enhanced_final_score': enhanced_final_score
    }

### 6. 가중치 비교 테스트 함수 (새로 추가)
def test_weight_impact():
    """
    가중치 범위 변경이 점수에 미치는 영향을 테스트
    """
    print("=== 가중치 범위 변경 영향 테스트 ===")
    
    # 가상의 데이터로 테스트
    test_data = [
        {'grade': 4.0, 'weight_small': 2, 'weight_large': 10},  # 2 → 10 (5배)
        {'grade': 3.0, 'weight_small': 5, 'weight_large': 25},  # 5 → 25 (5배)
        {'grade': 5.0, 'weight_small': 1, 'weight_large': 5},   # 1 → 5 (5배)
    ]
    
    # 작은 가중치로 계산
    numerator_small = sum(data['grade'] * data['weight_small'] for data in test_data)
    denominator_small = sum(data['weight_small'] for data in test_data)
    score_small = numerator_small / denominator_small
    
    # 큰 가중치로 계산
    numerator_large = sum(data['grade'] * data['weight_large'] for data in test_data)
    denominator_large = sum(data['weight_large'] for data in test_data)
    score_large = numerator_large / denominator_large
    
    print(f"작은 가중치 (1~5 범위): {score_small:.4f}")
    print(f"큰 가중치 (5~25 범위): {score_large:.4f}")
    print(f"차이: {abs(score_small - score_large):.6f}")
    print("✅ 가중치 범위가 달라도 점수는 동일합니다!")

### 7. 테스트 함수
def test_enhanced_scoring():
    """향상된 점수 계산 테스트 (team_goal 가중치 적용)"""
    print("=== team_goal 가중치 적용 점수 계산 테스트 ===")
    
    # 테스트 케이스
    test_cases = [
        {'user_id': 82, 'year': 2024, 'quarter': 1},
        {'user_id': 82, 'year': 2024, 'quarter': 2},
        {'user_id': 82, 'year': 2024, 'quarter': 3},
        {'user_id': 82, 'year': 2024, 'quarter': 4},
    ]
    
    for test_case in test_cases:
        print(f"\n--- 테스트: {test_case} ---")
        result = calculate_enhanced_final_score(**test_case)
        
        print(f"결과 요약:")
        print(f"  기존 최종 점수: {result['existing_final_score']}")
        print(f"  개인 실적 점수: {result['performance_score']}")
        print(f"  향상된 최종 점수: {result['enhanced_final_score']}")
        print("-" * 50)

### 8. 개별 테스트 실행 예시
if __name__ == "__main__":
    # 가중치 영향 테스트
    test_weight_impact()
    print("\n" + "="*60)
    
    # 기존 테스트
    evaluatee_id = 82
    year = 2024
    quarter = 4

    print("=== team_goal 가중치 적용 점수 계산 테스트 ===")
    avg = get_average_grade(evaluatee_id, year, quarter)
    workload = get_weighted_workload_score(evaluatee_id, year, quarter)
    final = calculate_final_score(avg, workload)

    print(f"[팀원 {evaluatee_id}] {year}년 {quarter}분기 평가")
    print(f"- 평균 점수: {avg}")
    print(f"- 업무량 점수 (team_goal): {workload}")
    print(f"- 최종 점수: {final}")
    
    print("\n" + "="*60)
    
    # 새로운 개인 실적 점수 테스트
    print("=== team_goal 가중치 적용 개인 실적 점수 테스트 ===")
    performance = get_personal_performance_score(evaluatee_id, quarter)
    print(f"사용자 {evaluatee_id}의 {quarter}분기 개인 실적 점수 (team_goal): {performance}")
    
    print("\n" + "="*60)
    
    # 향상된 최종 점수 계산 테스트
    print("=== team_goal 가중치 적용 향상된 최종 점수 계산 테스트 ===")
    enhanced_result = calculate_enhanced_final_score(evaluatee_id, year, quarter)
    
    print("\n📊 최종 결과 (team_goal 가중치 적용):")
    print(f"기존 점수 (25%): {enhanced_result['existing_final_score']}")
    print(f"실적 점수 (75%): {enhanced_result['performance_score']}")
    print(f"향상된 최종 점수: {enhanced_result['enhanced_final_score']}")
    
    # 전체 분기 테스트
    print("\n" + "="*60)
    test_enhanced_scoring()