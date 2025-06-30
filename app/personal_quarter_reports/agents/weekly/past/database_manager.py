import pymysql
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_config):
        self.db_config = db_config
    
    def connect(self):
        """MariaDB 연결"""
        try:
            connection = pymysql.connect(**self.db_config)
            logger.info("MariaDB 연결 성공")
            return connection
        except Exception as e:
            logger.error(f"MariaDB 연결 실패: {str(e)}")
            raise
    
    def load_team_data(self):
        """팀 기준 및 목표 데이터 로드"""
        logger.info("MariaDB에서 팀 데이터 로드 시작")
        
        connection = self.connect()
        try:
            # team_criteria 테이블 로드
            criteria_query = "SELECT * FROM team_criteria"
            team_criteria = pd.read_sql(criteria_query, connection)
            logger.info(f"team_criteria 로드 완료: {len(team_criteria)}건")
            
            # team_goal 테이블 로드
            goals_query = "SELECT * FROM team_goal"
            team_goals = pd.read_sql(goals_query, connection)
            logger.info(f"team_goal 로드 완료: {len(team_goals)}건")
            
            return team_criteria, team_goals
            
        finally:
            connection.close()
    
    def validate_tables(self):
        """테이블 존재 여부 확인"""
        connection = self.connect()
        cursor = connection.cursor()
        
        try:
            # 테이블 존재 확인
            cursor.execute("SHOW TABLES LIKE 'team_criteria'")
            criteria_exists = bool(cursor.fetchone())
            
            cursor.execute("SHOW TABLES LIKE 'team_goal'")
            goals_exists = bool(cursor.fetchone())
            
            if not criteria_exists:
                raise ValueError("team_criteria 테이블이 존재하지 않습니다.")
            if not goals_exists:
                raise ValueError("team_goal 테이블이 존재하지 않습니다.")
            
            # 레코드 수 확인
            cursor.execute("SELECT COUNT(*) FROM team_criteria")
            criteria_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM team_goal")
            goals_count = cursor.fetchone()[0]
            
            return {
                "team_criteria_records": criteria_count,
                "team_goals_records": goals_count
            }
        finally:
            connection.close()