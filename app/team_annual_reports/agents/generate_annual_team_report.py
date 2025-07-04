import os
import pymongo
import pymysql
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
import json

# 환경 변수 로드
load_dotenv()

class AnnualTeamGoalsUpdater:
    def __init__(self):
        self.maria_connection = None
        self.mongo_client = None
        self.mongo_db = None
        
    def connect_databases(self):
        """데이터베이스 연결"""
        try:
            # MariaDB 연결
            self.maria_connection = pymysql.connect(
                host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT')),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_NAME'),
                charset='utf8mb4'
            )
            print('✅ MariaDB 연결 성공')
            
            # MongoDB 연결
            mongo_url = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/{os.getenv('MONGO_DB_NAME')}?authSource=admin"
            
            self.mongo_client = pymongo.MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
            self.mongo_client.admin.command('ping')
            self.mongo_db = self.mongo_client[os.getenv('MONGO_DB_NAME')]
            print('✅ MongoDB 연결 성공')
            
        except Exception as e:
            print(f'❌ 데이터베이스 연결 오류: {e}')
            raise e
    
    def disconnect_databases(self):
        """데이터베이스 연결 해제"""
        try:
            if self.maria_connection:
                self.maria_connection.close()
                print('✅ MariaDB 연결 해제')
            if self.mongo_client:
                self.mongo_client.close()
                print('✅ MongoDB 연결 해제')
        except Exception as e:
            print(f'❌ 데이터베이스 연결 해제 오류: {e}')
    
    def get_organization_id_mapping(self) -> Dict[str, int]:
        """조직명 → organization_id 매핑 생성 (확인된 매핑 사용)"""
        # 이미 확인된 매핑 결과 사용
        mapping = {
            'W1팀': 1, 
            'DT3팀': 2, 
            'Cloud3팀': 3, 
            'ESG팀': 4
        }
        print(f'📋 조직명 매핑: {mapping}')
        return mapping
    
    def get_annual_team_goals(self, org_id: int, year: int) -> List[Dict]:
        """연말 팀 목표 데이터 조회 (team_criteria + task_results 조인, team_criteria.weight 사용, annual 등급 사용)"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                # team_criteria + task_results JOIN하여 team_criteria.weight 사용, annual 컬럼 사용
                cursor.execute("""
                    SELECT 
                        tc.goal_name,
                        tc.target,
                        tr.annual as grade,
                        tc.weight
                    FROM team_criteria tc
                    LEFT JOIN task_results tr ON tc.id = tr.task_id
                    WHERE tc.organization_id = %s
                    ORDER BY tc.id
                """, (org_id,))
                
                team_goals_data = cursor.fetchall()
                
                if not team_goals_data:
                    print(f'⚠️ 조직 {org_id}의 연말 팀 목표 데이터가 없습니다.')
                    return []
                
                team_goals = []
                for goal in team_goals_data:
                    goal_name = goal['goal_name']
                    target = goal['target']
                    grade = goal['grade']
                    weight = goal['weight']
                    
                    # None이거나 빈 값 처리
                    if not goal_name:
                        continue
                        
                    # target을 배열로 변환 (줄바꿈이나 점으로 구분)
                    if target:
                        # 개행문자로 분할하거나 점으로 시작하는 항목들 분할
                        content_items = []
                        lines = target.split('\n')
                        for line in lines:
                            line = line.strip()
                            if line:
                                # 점으로 시작하는 경우 점 제거
                                if line.startswith('.'):
                                    line = line[1:].strip()
                                content_items.append(line)
                        
                        # 너무 긴 경우 첫 번째 항목만 사용하거나 적절히 축약
                        if len(content_items) > 3:
                            content_items = content_items[:3]
                        elif not content_items:
                            # 원본 target이 너무 길면 축약
                            if len(target) > 100:
                                content_items = [target[:100] + "..."]
                            else:
                                content_items = [target]
                    else:
                        content_items = ["목표 내용 없음"]
                    
                    # content_items의 각 항목이 너무 길면 축약
                    processed_content = []
                    for item in content_items:
                        if len(item) > 80:
                            processed_content.append(item[:77] + "...")
                        else:
                            processed_content.append(item)
                    
                    team_goal = {
                        "goalName": goal_name,
                        "content": processed_content,
                        "grade": grade if grade else "미평가",
                        "weight": weight if weight is not None else 0
                    }
                    
                    team_goals.append(team_goal)
                
                print(f'✅ 조직 {org_id}의 연말 팀 목표 {len(team_goals)}개 조회 완료 (team_criteria.weight 사용)')
                return team_goals
                
        except Exception as e:
            print(f'❌ 연말 팀 목표 조회 오류: {e}')
            import traceback
            traceback.print_exc()
            return []
    
    def get_existing_annual_team_reports(self) -> List[Dict]:
        """기존 연말 팀 리포트 조회"""
        try:
            collection = self.mongo_db['reports']
            
            # team-annual 타입 리포트 조회
            reports = list(collection.find({'type': 'team-annual'}))
            
            print(f'📋 기존 연말 팀 리포트 {len(reports)}개 발견')
            return reports
            
        except Exception as e:
            print(f'❌ 기존 연말 리포트 조회 오류: {e}')
            return []
    
    def update_annual_report_with_team_goals(self, report: Dict, org_mapping: Dict[str, int]) -> bool:
        """개별 연말 리포트에 teamGoals 추가 (team_criteria.weight 사용)"""
        try:
            # 리포트에서 조직명과 연도 정보 추출
            department = report['user']['department']
            year = report['evaluated_year']
            
            # 조직명으로 organization_id 찾기
            org_id = org_mapping.get(department)
            if not org_id:
                print(f'⚠️ {department}의 organization_id를 찾을 수 없습니다.')
                return False
            
            # 연말 팀 목표 조회 (annual 등급 사용, team_criteria.weight 사용)
            team_goals = self.get_annual_team_goals(org_id, year)
            
            # 리포트에 teamGoals 추가
            report['teamGoals'] = team_goals
            report['updated_at'] = datetime.now()
            
            # MongoDB 업데이트
            collection = self.mongo_db['reports']
            filter_query = {'_id': report['_id']}
            
            result = collection.replace_one(filter_query, report)
            
            if result.modified_count > 0:
                print(f'✅ {department} {year}년 연말: teamGoals {len(team_goals)}개 추가 완료 (team_criteria.weight 사용)')
                return True
            else:
                print(f'⚠️ {department} {year}년 연말: 업데이트되지 않음')
                return False
                
        except Exception as e:
            print(f'❌ 연말 리포트 업데이트 오류: {e}')
            return False
    
    def update_all_annual_reports_with_team_goals(self) -> Dict:
        """모든 기존 연말 리포트에 teamGoals 추가 (team_criteria.weight 사용)"""
        try:
            print(f'\n🚀 모든 연말 팀 리포트에 teamGoals 추가 시작 (team_criteria.weight 사용)')
            
            # 1. 조직명 매핑 조회
            org_mapping = self.get_organization_id_mapping()
            
            # 2. 기존 연말 리포트 조회
            existing_reports = self.get_existing_annual_team_reports()
            if not existing_reports:
                print("❌ 기존 연말 리포트가 없습니다.")
                return {'success': 0, 'failed': 0, 'total': 0}
            
            total_count = len(existing_reports)
            success_count = 0
            failed_count = 0
            
            print(f'📋 총 {total_count}개 연말 리포트 업데이트 시작')
            
            # 3. 각 리포트 업데이트
            for i, report in enumerate(existing_reports, 1):
                try:
                    department = report['user']['department']
                    year = report['evaluated_year']
                    
                    print(f'\n[{i}/{total_count}] {department} {year}년 연말 처리 중...')
                    
                    if self.update_annual_report_with_team_goals(report, org_mapping):
                        success_count += 1
                    else:
                        failed_count += 1
                        
                except Exception as e:
                    print(f'❌ 연말 리포트 {i} 처리 오류: {e}')
                    failed_count += 1
            
            print(f'\n🎉 연말 업데이트 완료!')
            print(f'✅ 성공: {success_count}개')
            print(f'❌ 실패: {failed_count}개')
            print(f'📈 성공률: {(success_count/total_count)*100:.1f}%' if total_count > 0 else '0%')
            
            return {
                'success': success_count,
                'failed': failed_count,
                'total': total_count,
                'success_rate': (success_count/total_count)*100 if total_count > 0 else 0
            }
            
        except Exception as e:
            print(f'❌ 전체 연말 업데이트 오류: {e}')
            import traceback
            traceback.print_exc()
            return {'success': 0, 'failed': 0, 'total': 0}
    
    def show_updated_annual_reports_summary(self):
        """업데이트된 연말 리포트 요약 확인 (weight 포함)"""
        try:
            print(f"\n📊 업데이트된 연말 팀 리포트 요약 (weight 포함)")
            print("="*80)
            
            collection = self.mongo_db['reports']
            
            # team-annual 타입 문서 조회
            team_docs = list(collection.find({'type': 'team-annual'}))
            
            if not team_docs:
                print("❌ 연말 팀 리포트가 없습니다.")
                return
            
            print(f"📋 총 {len(team_docs)}개의 연말 팀 리포트")
            
            # 연도별 그룹화
            by_year = {}
            for doc in team_docs:
                key = f"{doc['evaluated_year']}년 연말"
                if key not in by_year:
                    by_year[key] = []
                by_year[key].append(doc)
            
            total_goals = 0
            total_weight = 0
            for year_key, docs in sorted(by_year.items()):
                print(f"\n🗓️ {year_key}:")
                year_goals = 0
                year_weight = 0
                for doc in sorted(docs, key=lambda x: x['user']['userId']):
                    team_name = doc['user']['department']
                    leader_name = doc['user']['name']
                    final_score = doc.get('finalScore', 0)
                    member_count = len(doc.get('memberAnalysis', []))
                    team_goals = doc.get('teamGoals', [])
                    team_goals_count = len(team_goals)
                    
                    # 팀의 총 가중치 계산
                    team_weight = sum(goal.get('weight', 0) for goal in team_goals)
                    
                    year_goals += team_goals_count
                    year_weight += team_weight
                    
                    print(f"   {team_name} (팀장: {leader_name}): {final_score}점, {member_count}명, 목표 {team_goals_count}개, 총 가중치 {team_weight}")
                
                print(f"   📊 {year_key} 총 목표: {year_goals}개, 총 가중치: {year_weight}")
                total_goals += year_goals
                total_weight += year_weight
            
            print(f"\n🎯 전체 연말 팀 목표 총계: {total_goals}개, 전체 가중치 총계: {total_weight}")
            
        except Exception as e:
            print(f'❌ 연말 리포트 요약 확인 오류: {e}')
    
    def test_single_annual_team_goals(self, org_id: int = 1, year: int = 2024):
        """단일 조직 연말 팀 목표 테스트 (team_criteria.weight 사용)"""
        try:
            print(f'\n🧪 단일 조직 연말 팀 목표 테스트 (team_criteria.weight 사용): 조직 {org_id}, {year}년 연말')
            
            # 연말 팀 목표 조회 (annual 등급 사용, team_criteria.weight 사용)
            team_goals = self.get_annual_team_goals(org_id, year)
            
            if team_goals:
                print(f'\n🎯 조회된 연말 팀 목표 {len(team_goals)}개:')
                total_weight = 0
                for i, goal in enumerate(team_goals, 1):
                    weight = goal.get('weight', 0)
                    total_weight += weight
                    print(f'\n{i}. 목표명: {goal.get("goalName", "N/A")}')
                    print(f'   연말 등급: {goal.get("grade", "N/A")}')
                    print(f'   가중치: {weight}')
                    print(f'   내용: {goal.get("content", [])}')
                
                print(f'\n📊 총 가중치 합계: {total_weight}')
                
                # JSON 형태로 출력
                print(f'\n📄 JSON 형태 (weight 포함):')
                print(json.dumps(team_goals, ensure_ascii=False, indent=2))
            else:
                print(f'❌ 조직 {org_id}의 연말 팀 목표를 찾을 수 없습니다.')
                
        except Exception as e:
            print(f'❌ 단일 연말 팀 목표 테스트 오류: {e}')
            import traceback
            traceback.print_exc()
    
    def show_annual_team_goals_by_organization(self):
        """조직별 연말 팀 목표 상세 확인 (team_criteria.weight 사용)"""
        try:
            print(f'\n📋 조직별 연말 팀 목표 상세 확인 (team_criteria.weight 사용)')
            print("="*80)
            
            org_mapping = self.get_organization_id_mapping()
            
            for org_name, org_id in org_mapping.items():
                print(f'\n🏢 {org_name} (ID: {org_id}) - 2024년 연말')
                print("-" * 40)
                
                # 연말 목표 확인
                team_goals = self.get_annual_team_goals(org_id, 2024)
                total_weight = sum(goal.get('weight', 0) for goal in team_goals)
                print(f'  📅 연말: {len(team_goals)}개 목표, 총 가중치: {total_weight}')
                
                for i, goal in enumerate(team_goals, 1):
                    goal_name = goal.get('goalName', 'N/A')
                    grade = goal.get('grade', 'N/A')
                    weight = goal.get('weight', 0)
                    print(f'    {i}. {goal_name[:40]}{"..." if len(goal_name) > 40 else ""} (연말등급: {grade}, 가중치: {weight})')
                        
        except Exception as e:
            print(f'❌ 조직별 연말 목표 확인 오류: {e}')

    def show_sample_annual_report_with_team_goals(self):
        """샘플 연말 리포트의 teamGoals 구조 확인 (weight 포함)"""
        try:
            print(f'\n📄 샘플 연말 리포트의 teamGoals 구조 확인 (weight 포함)')
            print("="*80)
            
            collection = self.mongo_db['reports']
            
            # 첫 번째 연말 리포트 조회
            sample_report = collection.find_one({'type': 'team-annual'})
            
            if sample_report:
                department = sample_report['user']['department']
                year = sample_report['evaluated_year']
                team_goals = sample_report.get('teamGoals', [])
                
                print(f'📋 샘플: {department} {year}년 연말')
                print(f'🎯 teamGoals 개수: {len(team_goals)}개')
                
                if team_goals:
                    # 가중치 통계
                    total_weight = sum(goal.get('weight', 0) for goal in team_goals)
                    print(f'⚖️ 총 가중치: {total_weight}')
                    
                    print(f'\n📄 teamGoals JSON 구조 (weight 포함):')
                    print(json.dumps(team_goals, ensure_ascii=False, indent=2))
                else:
                    print('⚠️ teamGoals가 비어있습니다.')
            else:
                print('❌ 샘플 연말 리포트를 찾을 수 없습니다.')
                
        except Exception as e:
            print(f'❌ 샘플 연말 리포트 확인 오류: {e}')


def main():
    """메인 실행 함수"""
    updater = AnnualTeamGoalsUpdater()
    
    try:
        updater.connect_databases()
        
        # 1. 단일 연말 팀 목표 테스트 (team_criteria.weight 사용)
        print("="*80)
        print("🧪 단일 연말 팀 목표 테스트 (team_criteria.weight 사용)")
        print("="*80)
        updater.test_single_annual_team_goals(org_id=1, year=2024)
        
        # 2. 조직별 연말 목표 상세 확인 (team_criteria.weight 사용)
        updater.show_annual_team_goals_by_organization()
        
        # 3. 모든 연말 리포트 업데이트 (team_criteria.weight 사용)
        print("\n" + "="*80)
        print("🔄 모든 연말 리포트 teamGoals 업데이트 (team_criteria.weight 사용)")
        print("="*80)
        result = updater.update_all_annual_reports_with_team_goals()
        
        print(f'\n📊 최종 연말 업데이트 결과:')
        print(f'   - 성공: {result["success"]}개')
        print(f'   - 실패: {result["failed"]}개') 
        print(f'   - 전체: {result["total"]}개')
        print(f'   - 성공률: {result["success_rate"]:.1f}%')
        
        # 4. 업데이트된 연말 리포트 요약 확인 (weight 포함)
        updater.show_updated_annual_reports_summary()
        
        # 5. 샘플 연말 리포트 구조 확인 (weight 포함)
        updater.show_sample_annual_report_with_team_goals()
        
    except Exception as e:
        print(f'❌ 메인 처리 오류: {e}')
        import traceback
        traceback.print_exc()
    finally:
        updater.disconnect_databases()


if __name__ == '__main__':
    main()