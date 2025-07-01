import os
import pymongo
import pymysql
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
import json

# 환경 변수 로드
load_dotenv()

class TeamQuarterReportGenerator:
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
    
    def get_organization_info(self, org_id: int) -> Dict:
        """조직 정보 조회"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                # 조직명 조회
                cursor.execute("""
                    SELECT name as department
                    FROM organizations 
                    WHERE division_id = %s
                """, (org_id,))
                org_info = cursor.fetchone()
                
                if not org_info:
                    return {'department': f'조직{org_id}'}
                
                return org_info
                
        except Exception as e:
            print(f'❌ 조직 정보 조회 오류: {e}')
            return {'department': f'조직{org_id}'}
    
    def get_team_leader_info(self, org_id: int) -> Dict:
        """팀장 정보 조회 (해당 팀의 첫 번째 사용자)"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("""
                    SELECT id as userId, name
                    FROM users 
                    WHERE organization_id = %s
                    ORDER BY id ASC
                    LIMIT 1
                """, (org_id,))
                
                leader = cursor.fetchone()
                
                if not leader:
                    return {'userId': 0, 'name': '팀장 미지정'}
                
                return leader
                
        except Exception as e:
            print(f'❌ 팀장 정보 조회 오류: {e}')
            return {'userId': 0, 'name': '팀장 미지정'}
    
    def get_member_analysis(self, org_id: int, year: int, quarter: int) -> List[Dict]:
        """팀 멤버 분석 데이터 조회 (team_ranking_info.py 결과)"""
        try:
            collection = self.mongo_db['ranking_results']
            
            document = collection.find_one({
                'type': 'team-quarter',
                'organization_id': org_id,
                'evaluated_year': year,
                'evaluated_quarter': quarter
            })
            
            if not document or 'memberAnalysis' not in document:
                print(f'⚠️ 조직 {org_id}의 {year}년 {quarter}분기 멤버 분석 데이터가 없습니다.')
                return []
            
            return document['memberAnalysis']
            
        except Exception as e:
            print(f'❌ 멤버 분석 데이터 조회 오류: {e}')
            return []
    
    def get_hr_suggestions(self, department: str, year: int, quarter: int) -> List[Dict]:
        """HR 제안사항 생성 (team_strategic_observations 결과 활용)"""
        try:
            collection = self.mongo_db['team_strategic_observations']
            
            # 먼저 해당 분기 데이터 조회
            document = collection.find_one({
                'division_name': department,
                'evaluated_year': year,
                'evaluated_quarter': quarter
            })
            
            # 해당 분기 데이터가 없으면 4분기 데이터로 fallback
            if not document and quarter != 4:
                print(f'⚠️ {department}의 {year}년 {quarter}분기 데이터가 없어 4분기 데이터를 사용합니다.')
                document = collection.find_one({
                    'division_name': department,
                    'evaluated_year': year,
                    'evaluated_quarter': 4
                })
            
            if not document:
                print(f'⚠️ {department}의 {year}년 전략적 관찰 데이터가 없습니다.')
                return []
            
            suggestions = []
            
            # 상위 성과자 제안 (finalComment 활용)
            top_performers = document.get('top_performers', [])
            if top_performers and len(top_performers) > 0:
                # 상위 성과자 중 1-2명 선별하여 제안
                selected_top = top_performers[:min(2, len(top_performers))]
                for performer in selected_top:
                    user_name = performer.get('user_name', '')
                    final_comment = performer.get('finalComment', '')
                    if user_name and final_comment:
                        # finalComment에서 첫 번째 문장이나 적절한 길이로 추출
                        sentences = final_comment.split('.')
                        recommendation = sentences[0] + '.' if sentences else final_comment
                        if len(recommendation) > 150:
                            recommendation = recommendation[:147] + "..."
                        
                        suggestions.append({
                            'target': user_name,
                            'recommendation': recommendation
                        })
            
            # 하위 성과자 제안 (finalComment 활용)
            bottom_performers = document.get('bottom_performers', [])
            if bottom_performers and len(bottom_performers) > 0:
                # 하위 성과자들을 개별적으로 처리
                selected_bottom = bottom_performers[-min(2, len(bottom_performers)):]  # 하위 1-2명
                for performer in selected_bottom:
                    user_name = performer.get('user_name', '')
                    final_comment = performer.get('finalComment', '')
                    if user_name and final_comment:
                        # finalComment에서 개선 필요 부분 추출
                        sentences = final_comment.split('.')
                        # "개선이 필요한" 또는 "보완"이 포함된 문장 찾기
                        improvement_sentence = None
                        for sentence in sentences:
                            if '개선' in sentence or '보완' in sentence or '향상' in sentence:
                                improvement_sentence = sentence.strip() + '.'
                                break
                        
                        if not improvement_sentence and sentences:
                            improvement_sentence = sentences[-1].strip() + '.'  # 마지막 문장 사용
                        
                        recommendation = improvement_sentence if improvement_sentence else "개별 역량 강화 프로그램 참여를 권장합니다."
                        
                        if len(recommendation) > 150:
                            recommendation = recommendation[:147] + "..."
                        
                        suggestions.append({
                            'target': user_name,
                            'recommendation': recommendation
                        })
            
            print(f'✅ {department} HR 제안사항 {len(suggestions)}개 생성 완료')
            return suggestions
            
        except Exception as e:
            print(f'❌ HR 제안사항 조회 오류: {e}')
            return []
    
    def get_org_suggestions(self, org_id: int, year: int, quarter: int) -> Dict:
        """조직 제안사항 조회 (team_negative_peer_solution.py 결과 활용)"""
        try:
            collection = self.mongo_db['peer_evaluation_results']
            
            document = collection.find_one({
                'type': 'team_quarter',
                'organization': org_id,
                'evaluated_year': year,
                'evaluated_quarter': quarter
            })
            
            if not document:
                print(f'⚠️ 조직 {org_id}의 {year}년 {quarter}분기 부정적 키워드 분석 데이터가 없습니다.')
                return {
                    'suggestion': '분석 데이터가 부족합니다.'
                }
            
            # 상위 부정 키워드들 추출
            top_keywords = document.get('top_negative_keywords', [])
            
            if not top_keywords:
                return {
                    'suggestion': '특별한 개선사항이 발견되지 않았습니다.'
                }
            
            # 상위 3개 키워드로 제안 생성
            keyword_names = [kw['keyword'] for kw in top_keywords[:3]]
            suggestion = f"구성원 간 공통 개선 키워드: {', '.join(keyword_names)}"
            
            return {
                'suggestion': suggestion
            }
            
        except Exception as e:
            print(f'❌ 조직 제안사항 조회 오류: {e}')
            return {
                'suggestion': '데이터 조회 중 오류가 발생했습니다.'
            }
    
    def get_final_comment(self, department: str, year: int, quarter: int, org_id: int) -> str:
        """최종 코멘트 조회 (peer_evaluation_results의 improvement_recommendations 활용)"""
        try:
            collection = self.mongo_db['peer_evaluation_results']
            
            # organization_name과 department 매칭하여 동일 부서 찾기
            document = collection.find_one({
                'type': 'team_quarter',
                'organization_name': department,
                'evaluated_year': year,
                'evaluated_quarter': quarter
            })
            
            # organization_name으로 찾지 못하면 organization ID로 시도
            if not document:
                document = collection.find_one({
                    'type': 'team_quarter',
                    'organization': org_id,
                    'evaluated_year': year,
                    'evaluated_quarter': quarter
                })
            
            if not document:
                print(f'⚠️ {department}의 {year}년 {quarter}분기 peer evaluation 데이터가 없습니다.')
                return f"{department}는 {year}년 {quarter}분기에 안정적인 성과를 기록했으며, 지속적인 개선을 통해 더욱 발전할 것으로 기대됩니다."
            
            improvement_recommendations = document.get('improvement_recommendations', '')
            
            if not improvement_recommendations:
                print(f'⚠️ {department}의 improvement_recommendations가 비어있습니다.')
                return f"{department}는 {year}년 {quarter}분기에 전반적으로 양호한 성과를 보였습니다."
            
            # improvement_recommendations 내용을 최종 코멘트로 사용
            # 너무 긴 경우 적절히 요약
            if len(improvement_recommendations) > 400:
                # 첫 번째 문단이나 적절한 길이로 자르기
                lines = improvement_recommendations.split('\n')
                first_paragraph = next((line.strip() for line in lines if line.strip() and len(line.strip()) > 50), improvement_recommendations)
                
                if len(first_paragraph) > 400:
                    # 첫 번째 문장들로 자르기
                    sentences = first_paragraph.split('.')
                    result = ""
                    for sentence in sentences:
                        if len(result + sentence + '.') <= 400:
                            result += sentence + '.'
                        else:
                            break
                    
                    if len(result) < 100:  # 너무 짧으면 원본의 400자 사용
                        result = improvement_recommendations[:397] + "..."
                        
                    return result
                else:
                    return first_paragraph
            else:
                return improvement_recommendations
            
        except Exception as e:
            print(f'❌ 최종 코멘트 조회 오류: {e}')
            return f"{department}는 {year}년 {quarter}분기에 지속적인 성장을 보여주고 있습니다."
    
    def calculate_team_final_score(self, member_analysis: List[Dict]) -> float:
        """팀 평균 점수 계산"""
        if not member_analysis:
            return 0.0
        
        total_score = sum(member.get('score', 0) for member in member_analysis)
        return round(total_score / len(member_analysis), 1)
    
    def get_quarter_dates(self, year: int, quarter: int) -> tuple:
        """분기별 시작/종료 날짜 계산"""
        quarter_dates = {
            1: (f"{year}-01-01", f"{year}-03-31"),
            2: (f"{year}-04-01", f"{year}-06-30"),
            3: (f"{year}-07-01", f"{year}-09-30"),
            4: (f"{year}-10-01", f"{year}-12-31")
        }
        return quarter_dates.get(quarter, (f"{year}-01-01", f"{year}-12-31"))
    
    def generate_team_quarter_report(self, org_id: int, year: int, quarter: int) -> Dict:
        """최종 팀 분기 리포트 생성"""
        try:
            print(f'\n🔄 조직 {org_id}의 {year}년 {quarter}분기 리포트 생성 시작')
            
            # 1. 기본 조직 정보 조회
            org_info = self.get_organization_info(org_id)
            department = org_info['department']
            
            # 2. 팀장 정보 조회
            team_leader = self.get_team_leader_info(org_id)
            team_leader['department'] = department
            
            # 3. 멤버 분석 데이터 조회
            member_analysis = self.get_member_analysis(org_id, year, quarter)
            
            if not member_analysis:
                print(f'❌ 조직 {org_id}의 멤버 분석 데이터가 없어 리포트 생성을 중단합니다.')
                return None
            
            # 4. 팀 평균 점수 계산
            final_score = self.calculate_team_final_score(member_analysis)
            
            # 5. 분기 날짜 계산
            start_date, end_date = self.get_quarter_dates(year, quarter)
            
            # 6. HR 제안사항 조회
            hr_suggestions = self.get_hr_suggestions(department, year, quarter)
            
            # 7. 조직 제안사항 조회
            org_suggestions = self.get_org_suggestions(org_id, year, quarter)
            
            # 8. 최종 코멘트 조회 (org_id 매개변수 포함)
            final_comment = self.get_final_comment(department, year, quarter, org_id)
            
            # 9. 최종 리포트 구성
            report = {
                'type': 'team-quarter',
                'evaluated_year': year,
                'evaluated_quarter': quarter,
                'title': f'{year}년 {quarter}분기 {department} 분기 리포트',
                'created_at': datetime.now().strftime('%Y-%m-%d'),
                'startDate': start_date,
                'endDate': end_date,
                'user': team_leader,
                'finalScore': final_score,
                'memberAnalysis': member_analysis,
                'hrSuggestions': hr_suggestions,
                'orgSuggestions': org_suggestions,
                'finalComment': final_comment
            }
            
            print(f'✅ 조직 {org_id} ({department}) 리포트 생성 완료')
            print(f'   - 팀원 수: {len(member_analysis)}명')
            print(f'   - 팀 평균 점수: {final_score}점')
            print(f'   - HR 제안: {len(hr_suggestions)}개')
            
            return report
            
        except Exception as e:
            print(f'❌ 조직 {org_id} 리포트 생성 오류: {e}')
            import traceback
            traceback.print_exc()
            return None
    
    def save_team_report_to_mongodb(self, report: Dict) -> bool:
        """팀 리포트를 MongoDB reports 컬렉션에 저장"""
        try:
            if not report:
                return False
            
            collection = self.mongo_db['reports']
            
            # 기존 데이터가 있으면 업데이트, 없으면 삽입
            filter_query = {
                'type': 'team-quarter',
                'user.userId': report['user']['userId'],  # 팀장 ID 기준
                'evaluated_year': report['evaluated_year'],
                'evaluated_quarter': report['evaluated_quarter']
            }
            
            # created_at, updated_at 추가
            report['created_at'] = datetime.now()
            report['updated_at'] = datetime.now()
            
            result = collection.replace_one(filter_query, report, upsert=True)
            
            org_name = report['user']['department']
            year = report['evaluated_year']
            quarter = report['evaluated_quarter']
            
            if result.upserted_id:
                print(f'✅ {org_name} {year}년 {quarter}분기 리포트 신규 저장: {result.upserted_id}')
            else:
                print(f'✅ {org_name} {year}년 {quarter}분기 리포트 업데이트 완료')
            
            return True
            
        except Exception as e:
            print(f'❌ 팀 리포트 저장 오류: {e}')
            return False
    
    def get_available_quarters(self) -> List[tuple]:
        """처리 가능한 모든 분기 조회 (team-quarter 타입만, team-annual 제외)"""
        try:
            collection = self.mongo_db['ranking_results']
            pipeline = [
                {"$match": {"type": "team-quarter"}},  # team-annual 제외
                {"$group": {
                    "_id": {
                        "year": "$evaluated_year", 
                        "quarter": "$evaluated_quarter"
                    }
                }},
                {"$sort": {"_id.year": 1, "_id.quarter": 1}}
            ]
            
            quarters = list(collection.aggregate(pipeline))
            quarter_list = [(q['_id']['year'], q['_id']['quarter']) for q in quarters]
            
            print(f"📅 처리 가능한 분기: {quarter_list}")
            return quarter_list
            
        except Exception as e:
            print(f'❌ 분기 데이터 조회 오류: {e}')
            return []
    
    def get_available_organizations(self) -> List[int]:
        """처리 가능한 모든 조직 조회"""
        try:
            collection = self.mongo_db['ranking_results']
            pipeline = [
                {"$match": {"type": "team-quarter"}},  # team-annual 제외
                {"$group": {"_id": "$organization_id"}},
                {"$sort": {"_id": 1}}
            ]
            
            orgs = list(collection.aggregate(pipeline))
            org_list = [org['_id'] for org in orgs if org['_id'] is not None]
            
            print(f"🏢 처리 가능한 조직: {org_list}")
            return org_list
            
        except Exception as e:
            print(f'❌ 조직 데이터 조회 오류: {e}')
            return []
    
    def generate_all_team_reports_all_quarters(self) -> Dict:
        """모든 팀의 모든 분기 리포트 생성 및 저장"""
        try:
            print(f'\n🚀 모든 팀 모든 분기 리포트 생성 시작')
            
            # 1. 처리 가능한 분기 목록 조회
            available_quarters = self.get_available_quarters()
            
            # 2. 처리 가능한 조직 목록 조회
            available_orgs = self.get_available_organizations()
            
            if not available_quarters or not available_orgs:
                print("❌ 처리할 데이터가 없습니다.")
                return {'success': 0, 'failed': 0, 'total': 0}
            
            total_tasks = len(available_quarters) * len(available_orgs)
            print(f'📋 총 처리 대상: {len(available_orgs)}개 조직 × {len(available_quarters)}개 분기 = {total_tasks}개 작업')
            
            success_count = 0
            failed_count = 0
            
            # 3. 분기별로 모든 조직 처리
            for year, quarter in available_quarters:
                print(f'\n{"="*60}')
                print(f'📅 {year}년 {quarter}분기 처리 시작')
                print(f'{"="*60}')
                
                quarter_success = 0
                quarter_failed = 0
                
                for org_id in available_orgs:
                    try:
                        # 리포트 생성
                        report = self.generate_team_quarter_report(org_id, year, quarter)
                        
                        if report:
                            # MongoDB에 저장
                            if self.save_team_report_to_mongodb(report):
                                success_count += 1
                                quarter_success += 1
                            else:
                                failed_count += 1
                                quarter_failed += 1
                        else:
                            failed_count += 1
                            quarter_failed += 1
                            
                    except Exception as e:
                        print(f'❌ 조직 {org_id} 처리 오류: {e}')
                        failed_count += 1
                        quarter_failed += 1
                
                print(f'📊 {year}년 {quarter}분기 결과: 성공 {quarter_success}개, 실패 {quarter_failed}개')
            
            print(f'\n🎉 전체 처리 완료!')
            print(f'✅ 총 성공: {success_count}개')
            print(f'❌ 총 실패: {failed_count}개')
            print(f'📈 성공률: {(success_count/total_tasks)*100:.1f}%' if total_tasks > 0 else '0%')
            
            return {
                'success': success_count,
                'failed': failed_count, 
                'total': total_tasks,
                'success_rate': (success_count/total_tasks)*100 if total_tasks > 0 else 0
            }
            
        except Exception as e:
            print(f'❌ 전체 팀 리포트 생성 오류: {e}')
            import traceback
            traceback.print_exc()
            return {'success': 0, 'failed': 0, 'total': 0}
    
    def show_saved_reports_summary(self):
        """저장된 리포트 요약 확인"""
        try:
            print(f"\n📊 저장된 팀 리포트 요약")
            print("="*60)
            
            collection = self.mongo_db['reports']
            
            # team-quarter 타입 문서 조회
            team_docs = list(collection.find({'type': 'team-quarter'}))
            
            if not team_docs:
                print("❌ 저장된 팀 리포트가 없습니다.")
                return
            
            print(f"📋 총 {len(team_docs)}개의 팀 리포트 저장됨")
            
            # 분기별 그룹화
            by_quarter = {}
            for doc in team_docs:
                key = f"{doc['evaluated_year']}년 {doc['evaluated_quarter']}분기"
                if key not in by_quarter:
                    by_quarter[key] = []
                by_quarter[key].append(doc)
            
            for quarter_key, docs in sorted(by_quarter.items()):
                print(f"\n🗓️ {quarter_key}:")
                for doc in sorted(docs, key=lambda x: x['user']['userId']):
                    team_name = doc['user']['department']
                    leader_name = doc['user']['name']
                    final_score = doc.get('finalScore', 0)
                    member_count = len(doc.get('memberAnalysis', []))
                    print(f"   {team_name} (팀장: {leader_name}): {final_score}점, {member_count}명")
            
        except Exception as e:
            print(f'❌ 리포트 요약 확인 오류: {e}')


def main():
    """메인 실행 함수"""
    generator = TeamQuarterReportGenerator()
    
    try:
        generator.connect_databases()
        
        # 모든 팀의 모든 분기 리포트 생성 및 MongoDB 저장
        result = generator.generate_all_team_reports_all_quarters()
        
        print(f'\n📊 최종 처리 결과:')
        print(f'   - 성공: {result["success"]}개')
        print(f'   - 실패: {result["failed"]}개') 
        print(f'   - 전체: {result["total"]}개')
        print(f'   - 성공률: {result["success_rate"]:.1f}%')
        
        # 저장된 리포트 요약 확인
        generator.show_saved_reports_summary()
        
        # 단일 테스트용 (필요시 주석 해제)
        # org_id = 1
        # year = 2024
        # quarter = 3
        # report = generator.generate_team_quarter_report(org_id, year, quarter)
        # if report:
        #     generator.save_team_report_to_mongodb(report)
        #     print(f'✅ 테스트 리포트 저장 완료')
        
    except Exception as e:
        print(f'❌ 메인 처리 오류: {e}')
        import traceback
        traceback.print_exc()
    finally:
        generator.disconnect_databases()


if __name__ == '__main__':
    main()