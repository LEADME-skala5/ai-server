class WeeklyReportEvaluator:
    def __init__(self, config=None):
        self.config = config or Config()
        
        # 컴포넌트 초기화
        print("📊 데이터베이스 연결 중...")
        self.db_manager = DatabaseManager(self.config.DB_CONFIG)
        
        print("🔍 Pinecone 연결 중...")
        self.pinecone_manager = PineconeManager(
            self.config.PINECONE_API_KEY, 
            self.config.PINECONE_INDEX_NAME
        )
        
        print("🤖 LLM 초기화 중...")
        self.llm_evaluator = LLMEvaluator(
            self.config.OPENAI_API_KEY, 
            self.config.MODEL
        )
        
        # 출력 디렉토리 생성
        self.output_path = Path(self.config.OUTPUT_PATH)
        self.output_path.mkdir(exist_ok=True)
        
        # 상태 관리
        self.evaluation_history = []
        
        logger.info("WeeklyReportEvaluator 초기화 완료")
    
    def validate_system(self, user_id=None):
        """시스템 전체 검증"""
        logger.info("시스템 검증 시작")
        
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": []
        }
        
        try:
            # MariaDB 검증
            print("📊 데이터베이스 검증 중...")
            db_validation = self.db_manager.validate_tables()
            validation_result["mariadb"] = db_validation
            
            # Pinecone 검증
            print("🔍 Pinecone 검증 중...")
            pinecone_validation = self.pinecone_manager.validate_connection(user_id)
            validation_result["pinecone"] = pinecone_validation
            
            logger.info("시스템 검증 완료")
            
        except Exception as e:
            validation_result["valid"] = False
            validation_result["errors"].append(str(e))
            logger.error(f"시스템 검증 실패: {e}")
        
        return validation_result
    
    def evaluate_single_user(self, user_id):
        """단일 사용자 평가"""
        logger.info(f"사용자 {user_id} 평가 시작")
        
        try:
            # 1. 시스템 검증
            print(f"1️⃣ 시스템 검증 중...")
            validation = self.validate_system(user_id)
            if not validation["valid"]:
                raise ValueError(f"시스템 검증 실패: {validation['errors']}")
            
            # 2. 데이터 로드
            print(f"2️⃣ 데이터 로드 중...")
            team_criteria, team_goals = self.db_manager.load_team_data()
            search_results = self.pinecone_manager.search_user_data(user_id)
            
            if not search_results.matches:
                raise ValueError(f"사용자 {user_id}의 데이터가 없습니다.")
            
            # 3. 데이터 처리
            print(f"3️⃣ 데이터 처리 중...")
            weekly_data = DataProcessor.convert_pinecone_to_dataframe(search_results)
            employee_info = DataProcessor.extract_employee_info(weekly_data)
            
            # 주간 업무 데이터 추가
            employee_info['weekly_tasks'] = weekly_data[['start_date', 'end_date', 'done_task']].to_dict('records')
            
            # 팀 데이터 필터링
            org_keywords = ['organization_id', 'org_id', 'team_id', '조직', '팀']
            filtered_goals = DataProcessor.filter_team_data_by_org(
                team_goals, employee_info['organization_id'], org_keywords
            )
            filtered_criteria = DataProcessor.filter_team_data_by_org(
                team_criteria, employee_info['organization_id'], org_keywords
            )
            
            # 4. LLM 평가
            print(f"4️⃣ AI 평가 실행 중...")
            prompt = self.llm_evaluator.generate_prompt(
                employee_info, filtered_goals, filtered_criteria
            )
            evaluation_result = self.llm_evaluator.execute_evaluation(prompt)
            
            # 5. 결과 저장
            if "error" not in evaluation_result:
                print(f"5️⃣ 결과 저장 중...")
                output_file = self._save_results(evaluation_result, user_id)
                
                self.evaluation_history.append({
                    "user_id": user_id,
                    "timestamp": datetime.now().isoformat(),
                    "output_file": output_file,
                    "status": "success"
                })
                
                logger.info(f"사용자 {user_id} 평가 완료")
                return evaluation_result
            else:
                raise ValueError(f"LLM 평가 실패: {evaluation_result['error']}")
                
        except Exception as e:
            logger.error(f"사용자 {user_id} 평가 실패: {e}")
            return {"error": str(e), "user_id": user_id}
    
    def evaluate_batch_users(self, user_ids=None):
        """배치 사용자 평가"""
        logger.info("배치 평가 시작")
        
        if user_ids is None:
            user_ids = self.pinecone_manager.get_available_user_ids()
        
        if not user_ids:
            return {"error": "사용 가능한 사용자가 없습니다."}
        
        batch_results = {
            "batch_metadata": {
                "start_time": datetime.now().isoformat(),
                "target_user_ids": user_ids,
                "total_users": len(user_ids)
            },
            "individual_results": {},
            "batch_summary": {
                "successful_evaluations": 0,
                "failed_evaluations": 0
            }
        }
        
        for i, user_id in enumerate(user_ids, 1):
            print(f"\n📊 배치 평가 진행: {i}/{len(user_ids)} - User {user_id}")
            
            result = self.evaluate_single_user(user_id)
            batch_results["individual_results"][user_id] = result
            
            if "error" not in result:
                batch_results["batch_summary"]["successful_evaluations"] += 1
                print(f"✅ User {user_id} 평가 성공")
            else:
                batch_results["batch_summary"]["failed_evaluations"] += 1
                print(f"❌ User {user_id} 평가 실패: {result['error']}")
        
        batch_results["batch_metadata"]["end_time"] = datetime.now().isoformat()
        
        # 배치 결과 저장
        self._save_batch_results(batch_results)
        
        logger.info(f"배치 평가 완료 - 성공: {batch_results['batch_summary']['successful_evaluations']}명")
        return batch_results
    
    def get_available_users(self):
        """사용 가능한 사용자 목록 조회"""
        return self.pinecone_manager.get_available_user_ids()
    
    def get_evaluation_statistics(self):
        """평가 통계 조회"""
        if not self.evaluation_history:
            return {"total_evaluations": 0}
        
        successful = [e for e in self.evaluation_history if e.get("status") == "success"]
        
        return {
            "total_evaluations": len(self.evaluation_history),
            "successful_evaluations": len(successful),
            "failed_evaluations": len(self.evaluation_history) - len(successful),
            "latest_evaluation": self.evaluation_history[-1]["timestamp"],
            "evaluated_users": [e["user_id"] for e in self.evaluation_history]
        }
    
    def _save_results(self, results, user_id):
        """결과 저장"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"evaluation_{user_id}_{timestamp}.json"
        
        # 참조 정보 추가
        results["reference"] = {
            "evaluation_timestamp": datetime.now().isoformat(),
            "user_id": user_id,
            "data_sources": {
                "weekly_data": f"Pinecone Index: {self.config.PINECONE_INDEX_NAME}",
                "team_data": f"MariaDB: {self.config.DB_CONFIG['host']}/{self.config.DB_CONFIG['database']}"
            }
        }
        
        output_file = self.output_path / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"결과 저장 완료: {output_file}")
        return str(output_file)
    
    def _save_batch_results(self, batch_results):
        """배치 결과 저장"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"batch_evaluation_{timestamp}.json"
        
        output_file = self.output_path / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(batch_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"배치 결과 저장 완료: {output_file}")