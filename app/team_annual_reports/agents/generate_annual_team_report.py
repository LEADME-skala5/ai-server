import os
import asyncio
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
import pymongo
import pymysql
from openai import OpenAI
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

class CompleteAnnualReportSystem:
    def __init__(self):
        self.maria_connection = None
        self.mongo_client = None
        self.mongo_db = None
        self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
    async def connect_databases(self):
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
            
            self.mongo_client = pymongo.MongoClient(
                mongo_url,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
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
    
    def get_organization_names(self) -> Dict[int, str]:
        """organizations 테이블에서 division_id와 name 매핑 조회"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                query = """
                    SELECT division_id, name
                    FROM organizations
                    WHERE division_id IS NOT NULL
                    GROUP BY division_id, name
                    ORDER BY division_id
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                
            org_name_mapping = {}
            for row in rows:
                division_id = row['division_id']
                org_name = row['name']
                org_name_mapping[division_id] = org_name
            
            print(f'🏢 조직 이름 매핑 조회 완료: {len(org_name_mapping)}개')
            return org_name_mapping
            
        except Exception as e:
            print(f'❌ 조직 이름 매핑 조회 오류: {e}')
            return {}
    
    def get_team_manager(self, org_id: int) -> Dict:
        """특정 조직의 팀장 정보 조회 (is_manager = 1)"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                query = """
                    SELECT id, name
                    FROM users 
                    WHERE organization_id = %s AND is_manager = 1
                    LIMIT 1
                """
                cursor.execute(query, (org_id,))
                manager = cursor.fetchone()
                
                if manager:
                    print(f'👑 조직 {org_id} 팀장: {manager["name"]} (ID: {manager["id"]})')
                    return {
                        'user_id': manager['id'],
                        'name': manager['name']
                    }
                else:
                    print(f'⚠️ 조직 {org_id}에 팀장(is_manager=1)이 없습니다.')
                    return {'user_id': 0, 'name': '팀장'}
                    
        except Exception as e:
            print(f'❌ 팀장 정보 조회 오류: {e}')
            return {'user_id': 0, 'name': '팀장'}
    
    def get_user_organization_mapping(self) -> Dict[int, int]:
        """사용자 ID별 조직 ID 매핑"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                query = """
                    SELECT id, organization_id
                    FROM users 
                    WHERE organization_id IS NOT NULL
                """
                cursor.execute(query)
                users = cursor.fetchall()
            
            user_org_mapping = {user['id']: user['organization_id'] for user in users}
            print(f'👥 사용자-조직 매핑 완료: {len(user_org_mapping)}명')
            return user_org_mapping
            
        except Exception as e:
            print(f'❌ 사용자-조직 매핑 오류: {e}')
            return {}
    
    def get_annual_final_scores(self, year: int) -> Dict[int, Dict]:
        """연말 최종 점수 데이터 가져오기"""
        try:
            collection = self.mongo_db['final_score_results']
            
            final_score_doc = collection.find_one({
                'type': 'personal-final-score-annual',
                'evaluated_year': year
            })
            
            if not final_score_doc:
                print(f"❌ {year}년 연말 최종 점수 데이터를 찾을 수 없습니다.")
                return {}
            
            users_scores = {}
            users_data = final_score_doc.get('users', {})
            
            for user_id_str, user_data in users_data.items():
                try:
                    user_id = int(user_id_str)
                    final_score_info = user_data.get('final_score_info', {})
                    category_averages = final_score_info.get('category_averages', {})
                    
                    overall_score = final_score_info.get('overall_final_score', 0)
                    quantitative = category_averages.get('quantitative', 0)
                    qualitative = category_averages.get('qualitative', 0)
                    peer = category_averages.get('peer', 0)
                    
                    users_scores[user_id] = {
                        'overall_final_score': round(overall_score, 1),
                        'quantitative': round(quantitative, 1),
                        'qualitative': round(qualitative, 1),
                        'peer': round(peer, 1)
                    }
                    
                except (ValueError, TypeError) as e:
                    print(f"⚠️ 사용자 {user_id_str} 점수 처리 오류: {e}")
                    continue
            
            print(f'✅ {year}년 연말 점수 데이터 로드 완료: {len(users_scores)}명')
            return users_scores
            
        except Exception as e:
            print(f'❌ 연말 점수 데이터 조회 오류: {e}')
            return {}
    
    def get_reports_by_organization(self, evaluated_year: int) -> Dict[str, List[Dict]]:
        """organization_id별로 personal-annual 보고서 조회"""
        try:
            reports_collection = self.mongo_db['reports']
            
            query = {
                'type': 'personal-annual',
                'evaluated_year': evaluated_year,
                'user.userId': {'$exists': True, '$ne': None}
            }
            
            reports = list(reports_collection.find(query))
            print(f'📋 {len(reports)}개의 개인 연말 보고서 조회 완료')
            
            if len(reports) == 0:
                return {}
            
            user_org_mapping = self.get_user_organization_mapping()
            
            org_reports = {}
            for report in reports:
                user_id = report.get('user', {}).get('userId')
                if user_id and user_id in user_org_mapping:
                    org_id = str(user_org_mapping[user_id])
                    if org_id not in org_reports:
                        org_reports[org_id] = []
                    org_reports[org_id].append(report)
            
            return org_reports
            
        except Exception as e:
            print(f'❌ 조직별 연말 보고서 조회 오류: {e}')
            return {}
    
    def classify_division_performance(self, reports: List[Dict]) -> Tuple[List[Dict], List[Dict], int]:
        """조직 내에서 개인별 finalScore 기준 상위/하위 20% 분류"""
        if not reports:
            return [], [], 0
        
        sorted_reports = sorted(reports, key=lambda x: x.get('finalScore', 0), reverse=True)
        
        total_count = len(sorted_reports)
        top_20_percent_count = math.ceil(total_count * 0.2)
        bottom_20_percent_count = math.ceil(total_count * 0.2)
        
        top_performers = sorted_reports[:top_20_percent_count]
        bottom_performers = sorted_reports[-bottom_20_percent_count:]
        
        return top_performers, bottom_performers, total_count
    
    async def generate_management_strategy(self, top_performers: List[Dict], bottom_performers: List[Dict], org_name: str) -> str:
        """GPT-4o를 사용한 조직별 맞춤 관리 방향 생성"""
        try:
            top_comments = [
                report.get('finalComment', '') 
                for report in top_performers 
                if report.get('finalComment', '').strip()
            ]
            
            bottom_comments = [
                report.get('finalComment', '') 
                for report in bottom_performers 
                if report.get('finalComment', '').strip()
            ]
            
            top_scores = [p.get('finalScore', 0) for p in top_performers]
            bottom_scores = [p.get('finalScore', 0) for p in bottom_performers]
            
            top_avg_score = sum(top_scores) / len(top_scores) if top_scores else 0
            bottom_avg_score = sum(bottom_scores) / len(bottom_scores) if bottom_scores else 0
            
            prompt = f"""
{org_name} 조직의 연말 성과 평가 분석 결과를 바탕으로 내년도 관리 전략을 작성해주세요.

상위 20% 성과자 ({len(top_performers)}명) - 평균 {top_avg_score:.1f}점
하위 20% 성과자 ({len(bottom_performers)}명) - 평균 {bottom_avg_score:.1f}점

상위 성과자 관리 전략과 하위 성과자 개선 방안, 그리고 조직 전체 발전 방향을 포함하여 작성해주세요.

중요: 응답에서 절대 사용하지 말아야 할 것들:
- 숫자 목록 (1., 2., 3., 4. 등)
- 불릿 포인트 (-, *, • 등)
- 마크다운 문법 (#, ##, **, * 등)
자연스러운 문단 형태의 텍스트로만 작성해주세요.
            """
            
            response = self.openai_client.chat.completions.create(
                model='gpt-4o',
                messages=[
                    {
                        'role': 'system',
                        'content': '조직 관리 전문가로서 연말 성과 분석을 바탕으로 실무적인 관리 전략을 제시해주세요. 번호, 기호, 마크다운 문법 없이 자연스러운 문단으로만 작성해주세요.'
                    },
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                temperature=0.7,
                max_tokens=1500
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            print(f'❌ {org_name} 조직 GPT 응답 생성 오류: {e}')
            return f"{org_name} 조직의 성과 분석을 바탕으로 상위 성과자의 강점을 조직 전체로 확산하고, 하위 성과자의 역량 개발을 통해 전체적인 조직 역량 향상을 도모해야 합니다."
    
    async def get_final_comment_from_strategic_observations(self, org_id: int, year: int) -> str:
        """team_strategic_observations 컬렉션에서 연말 관리 전략을 finalComment로 가져오기"""
        try:
            collection = self.mongo_db['team_strategic_observations']
            
            # evaluated_quarter 필드가 없는 연말 문서 조회
            strategic_doc = collection.find_one({
                'organization_id': str(org_id),
                'evaluated_year': year,
                'evaluated_quarter': {'$exists': False}  # evaluated_quarter 필드가 없는 문서
            })
            
            if strategic_doc and 'management_strategy' in strategic_doc:
                management_strategy = strategic_doc['management_strategy']
                print(f'📋 조직 {org_id} 연말 관리 전략을 finalComment로 설정')
                return management_strategy
            else:
                print(f'⚠️ 조직 {org_id}의 연말 관리 전략을 찾을 수 없습니다.')
                return f"조직의 연말 성과 분석을 바탕으로 지속적인 발전을 위한 전략을 수립하고 있습니다."
                
        except Exception as e:
            print(f'❌ 조직 {org_id} finalComment 조회 오류: {e}')
            return f"조직의 연말 성과 분석을 바탕으로 지속적인 발전을 위한 전략을 수립하고 있습니다."

    async def generate_hr_suggestions(self, org_id: int, year: int) -> List[Dict]:
        """HR 추천사항 생성 (상위/하위 성과자의 finalComment 활용)"""
        try:
            collection = self.mongo_db['team_strategic_observations']
            
            # evaluated_quarter 필드가 없는 연말 문서 조회
            strategic_doc = collection.find_one({
                'organization_id': str(org_id),
                'evaluated_year': year,
                'evaluated_quarter': {'$exists': False}
            })
            
            if not strategic_doc:
                print(f'⚠️ 조직 {org_id}의 연말 전략적 관찰 데이터가 없습니다.')
                return []
            
            suggestions = []
            
            # 상위 성과자 제안 (finalComment 그대로 사용)
            top_performers = strategic_doc.get('top_performers', [])
            if top_performers and len(top_performers) > 0:
                # 상위 성과자 중 1-2명 선별하여 제안
                selected_top = top_performers[:min(2, len(top_performers))]
                for performer in selected_top:
                    user_name = performer.get('user_name', '')
                    final_comment = performer.get('finalComment', '')
                    if user_name and final_comment.strip():
                        suggestions.append({
                            'target': user_name,
                            'recommendation': final_comment
                        })
            
            # 하위 성과자 제안 (finalComment 그대로 사용)
            bottom_performers = strategic_doc.get('bottom_performers', [])
            if bottom_performers and len(bottom_performers) > 0:
                # 하위 성과자들을 개별적으로 처리
                selected_bottom = bottom_performers[-min(2, len(bottom_performers)):]  # 하위 1-2명
                for performer in selected_bottom:
                    user_name = performer.get('user_name', '')
                    final_comment = performer.get('finalComment', '')
                    if user_name and final_comment.strip():
                        suggestions.append({
                            'target': user_name,
                            'recommendation': final_comment
                        })
            
            print(f'✅ 조직 {org_id} HR 제안사항 {len(suggestions)}개 생성 완료')
            return suggestions
            
        except Exception as e:
            print(f'❌ 조직 {org_id} HR 추천사항 생성 오류: {e}')
            return []
    
    async def generate_complete_annual_report(self, org_id: int, org_name: str, year: int) -> Dict:
        """완전한 연말 보고서 생성"""
        try:
            print(f'\n🔄 {org_name} 조직 완전한 연말 보고서 생성 시작')
            
            # 1. 연말 점수 데이터 가져오기
            annual_scores = self.get_annual_final_scores(year)
            if not annual_scores:
                print(f"❌ {year}년 연말 점수 데이터가 없습니다.")
                return None
            
            # 2. 사용자-조직 매핑
            user_org_mapping = self.get_user_organization_mapping()
            
            # 3. 해당 조직의 사용자들 필터링 및 정렬 (reports 컬렉션 사용)
            org_reports = self.get_reports_by_organization(year)
            reports = org_reports.get(str(org_id), [])
            
            if not reports:
                print(f"❌ 조직 {org_id}에 속한 연말 보고서가 없습니다.")
                return None
            
            # finalScore 기준으로 내림차순 정렬
            reports.sort(key=lambda x: x.get('finalScore', 0), reverse=True)
            
            # 4. memberAnalysis 생성 (reports 컬렉션의 finalScore 사용)
            member_analysis = []
            total_members = len(reports)
            
            for report in reports:
                user_name = report.get('user', {}).get('name', 'Unknown')
                final_score = report.get('finalScore', 0)
                
                # annual_scores에서 세부 점수 가져오기 (있는 경우에만)
                user_id = report.get('user', {}).get('userId')
                if user_id and user_id in annual_scores:
                    scores_detail = annual_scores[user_id]
                    quantitative = scores_detail.get('quantitative', 0)
                    qualitative = scores_detail.get('qualitative', 0)
                    peer = scores_detail.get('peer', 0)
                else:
                    # annual_scores에 없으면 기본값
                    quantitative = qualitative = peer = 0
                
                member_data = {
                    'name': user_name,
                    'scores': {
                        'Quantitative': quantitative,
                        'Qualitative': qualitative,
                        'Peer': peer
                    },
                    'finalScore': final_score  # reports 컬렉션의 finalScore 사용
                }
                member_analysis.append(member_data)
            
            # 5. 성과 분류 및 관리 전략 생성 (이미 가져온 reports 사용)
            top_performers, bottom_performers, total_count = self.classify_division_performance(reports)
            
            # 6. HR 추천사항 생성 (team_strategic_observations에서 상위/하위 성과자 활용)
            hr_suggestions = await self.generate_hr_suggestions(org_id, year)
            
            # 7. finalComment 생성 (team_strategic_observations의 management_strategy)
            final_comment = await self.get_final_comment_from_strategic_observations(org_id, year)
            
            # 8. 날짜 정보 생성
            current_date = datetime.now()
            start_date = datetime(year, 10, 1)  # 연말 보고서는 10월부터
            end_date = current_date
            
            # 9. 팀장 정보 (is_manager = 1인 사용자)
            team_leader = self.get_team_manager(org_id)
            
            # 10. 완전한 연말 보고서 구조 생성
            complete_report = {
                'type': 'team-annual',
                'evaluated_year': year,
                'title': f'{year} {org_name} 연말 리포트',
                'created_at': current_date.strftime('%Y-%m-%d'),
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d'),
                'user': {
                    'userId': team_leader['user_id'],
                    'name': team_leader['name'],
                    'department': org_name
                },
                'memberAnalysis': member_analysis,
                'hrSuggestions': hr_suggestions,
                'finalComment': final_comment,
                'organization_id': org_id,
                'organization_name': org_name,
                'total_members': total_members,
                'created_at_full': current_date,
                'updated_at': current_date
            }
            
            print(f'✅ {org_name} 완전한 연말 보고서 생성 완료')
            return complete_report
            
        except Exception as e:
            print(f'❌ {org_name} 완전한 연말 보고서 생성 오류: {e}')
            import traceback
            traceback.print_exc()
            return None
    
    def save_complete_annual_report(self, report_data: Dict) -> bool:
        """완전한 연말 보고서를 reports 컬렉션에 저장"""
        try:
            if not report_data:
                return False
            
            collection = self.mongo_db['reports']
            
            filter_query = {
                'type': 'team-annual',
                'organization_id': report_data['organization_id'],
                'evaluated_year': report_data['evaluated_year']
            }
            
            result = collection.replace_one(filter_query, report_data, upsert=True)
            
            if result.upserted_id:
                print(f'✅ 조직 {report_data["organization_id"]} 연말 보고서 신규 저장')
            else:
                print(f'✅ 조직 {report_data["organization_id"]} 연말 보고서 업데이트 완료')
            
            return True
            
        except Exception as e:
            print(f'❌ 연말 보고서 저장 오류: {e}')
            return False
    
    async def process_all_organizations_complete_reports(self, year: int = 2024):
        """모든 조직의 완전한 연말 보고서 생성"""
        try:
            print(f'\n🚀 {year}년 모든 조직 완전한 연말 보고서 생성 시작')
            
            await self.connect_databases()
            
            # 조직 이름 매핑 가져오기
            org_name_mapping = self.get_organization_names()
            
            total_success = 0
            total_fail = 0
            
            # 각 조직별로 처리
            for org_id, org_name in org_name_mapping.items():
                print(f'\n{"="*60}')
                print(f'📅 조직 {org_id} ({org_name}) 처리 시작')
                print(f'{"="*60}')
                
                # 완전한 연말 보고서 생성
                complete_report = await self.generate_complete_annual_report(org_id, org_name, year)
                
                if complete_report and self.save_complete_annual_report(complete_report):
                    total_success += 1
                    
                    # 생성된 보고서 요약 출력
                    print(f'📊 {org_name} 보고서 요약:')
                    print(f'   - 총 팀원: {complete_report["total_members"]}명')
                    print(f'   - 팀장: {complete_report["user"]["name"]}')
                    print(f'   - HR 추천사항: {len(complete_report["hrSuggestions"])}개')
                else:
                    total_fail += 1
                
                # OpenAI API 호출 제한을 고려한 지연
                await asyncio.sleep(1)
            
            print(f'\n🎉 모든 조직 완전한 연말 보고서 생성 완료!')
            print(f'✅ 총 성공: {total_success}개')
            print(f'❌ 총 실패: {total_fail}개')
            
        except Exception as e:
            print(f'❌ 전체 완전한 연말 보고서 처리 오류: {e}')
            raise e
        finally:
            self.disconnect_databases()


# 실행부
async def main():
    system = CompleteAnnualReportSystem()
    
    try:
        # 2024년 모든 조직의 완전한 연말 보고서 생성
        await system.process_all_organizations_complete_reports(2024)
        
    except Exception as e:
        print(f'❌ 메인 처리 오류: {e}')
        exit(1)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단됨")
    except Exception as e:
        print(f"❌ 실행 오류: {e}")
        exit(1)