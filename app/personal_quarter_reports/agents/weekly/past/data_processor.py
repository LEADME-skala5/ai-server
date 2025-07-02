class DataProcessor:
    @staticmethod
    def convert_pinecone_to_dataframe(search_results):
        """Pinecone 검색 결과를 DataFrame으로 변환"""
        weekly_records = []
        
        for match in search_results.matches:
            metadata = match.metadata
            
            record = {
                'employee_number': metadata.get('user_id'),
                'name': f"User_{metadata.get('user_id')}",
                'done_task': metadata.get('done_task', ''),
                'start_date': metadata.get('start_date'),
                'end_date': metadata.get('end_date'),
                'evaluation_year': metadata.get('evaluation_year'),
                'evaluation_quarter': metadata.get('evaluation_quarter'),
                'organization_id': metadata.get('organization_id'),
                'source_file': metadata.get('source_file', ''),
                'row_index': metadata.get('row_index', '')
            }
            weekly_records.append(record)
        
        df = pd.DataFrame(weekly_records)
        
        # 중복 제거
        if not df.empty:
            df = df.drop_duplicates(
                subset=['employee_number', 'start_date', 'end_date'], 
                keep='first'
            )
        
        return df
    
    @staticmethod
    def extract_employee_info(employee_data):
        """직원 기본 정보 추출"""
        info = {
            "name": employee_data['name'].iloc[0] if 'name' in employee_data.columns else f"User_{employee_data['employee_number'].iloc[0]}",
            "employee_number": employee_data['employee_number'].iloc[0],
            "organization_id": employee_data['organization_id'].iloc[0] if 'organization_id' in employee_data.columns else "",
            "evaluation_year": employee_data['evaluation_year'].iloc[0] if 'evaluation_year' in employee_data.columns else "",
            "evaluation_quarter": employee_data['evaluation_quarter'].iloc[0] if 'evaluation_quarter' in employee_data.columns else "",
            "total_weeks": len(employee_data),
            "total_activities": len(employee_data)
        }
        
        # 날짜 범위 설정
        if 'start_date' in employee_data.columns and 'end_date' in employee_data.columns:
            start_dates = pd.to_datetime(employee_data['start_date'], errors='coerce').dropna()
            end_dates = pd.to_datetime(employee_data['end_date'], errors='coerce').dropna()
            if not start_dates.empty and not end_dates.empty:
                info["period"] = f"{start_dates.min().strftime('%Y-%m-%d')} ~ {end_dates.max().strftime('%Y-%m-%d')}"
        
        return info
    
    @staticmethod
    def filter_team_data_by_org(team_data, org_id, org_keywords):
        """조직 ID로 팀 데이터 필터링"""
        if team_data is None or team_data.empty or not org_id:
            return team_data.to_dict('records') if team_data is not None else []
        
        # organization_id 컬럼 찾기
        org_column = None
        for col in team_data.columns:
            col_lower = str(col).lower().strip()
            if any(keyword.lower() in col_lower for keyword in org_keywords):
                org_column = col
                break
        
        if org_column:
            filtered_data = team_data[
                team_data[org_column].astype(str) == str(org_id)
            ].to_dict('records')
            logger.info(f"팀 데이터 필터링 완료: {len(filtered_data)}개")
            return filtered_data
        
        logger.warning("조직 ID 매칭 실패, 전체 데이터 반환")
        return team_data.to_dict('records')
