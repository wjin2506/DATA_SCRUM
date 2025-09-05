#!/usr/bin/env python3

"""
AUTO-RESTART MAXIMUM DATA EXTRACTION
5분 이상 멈추면 자동으로 끄고 중단된 지점부터 재시작
"""

import os
import csv
import time
import signal
import threading
import subprocess
import pandas as pd
import google.generativeai as genai
from datetime import datetime
from pathlib import Path

# API 키 설정
api_key = os.environ.get('GEMINI_API_KEY')
if not api_key:
    print("GEMINI_API_KEY 환경변수를 설정하세요")
    exit(1)

genai.configure(api_key=api_key)
model = genai.GenerativeModel('gemini-2.5-flash')

# 선박 의약품 목록 (medi.md 기반)
SHIP_MEDICINES = """
주사약:
- 항생제: 클로람페니콜, 황산카나마이신, 아목시실린
- 강심제: 아미노필린
- 혈관수축제: 에피네프린
- 국소마취제: 염산리도카인
- 지혈제: 파라아미노메칠벤조산
- 해열진통소염제: 케토프로펜
- 수액제: 생리식염주사액, 5%당 링겔, 주사용 증류수

내용약:
- 항생제: 아목시실린, 독시싸이클린, 클로람페니콜
- 해열진통소염제: 아세트아미노펜, 아스피린, 디클로페낙나트륨
- 감기약: 종합감기약
- 진해거담제: 카보시스테인, 용각산
- 진정제: 독실아민숙시네이트
- 진훈제(멀미약): 디메칠하이드리에이트
- 소화제: 훼스탈포르테, 베아제, 활명수
- 제산제: 암포젤엠현탁액, 탈시드
- 진경제: 복합부스코판
- 정장제: 로페라미드, 정로환
- 해독제: 오로친정, 메치오닌
- 비타민: 레모나세립, 비콤푸렉스

외용약:
- 화농성질환용: 테라마이신연고, 황산겐타마이신연고
- 안과용약: 뷰렌점안액, 테라코트릴눈/귀약
- 진통진양제: 훼너간크림, 맥살겔연고, 카라민로숀
- 소독제: 베타딘액, 포비돈액, 과산화수소, 소독용알콜
- 창상보호제: 화상가아제
"""

class AutoRestartAnalyzer:
    def __init__(self):
        self.last_activity_time = time.time()
        self.activity_lock = threading.Lock()
        self.should_stop = False
        self.watchdog_thread = None
        self.current_batch = 0
        self.processed_cargos = set()
        
    def update_activity(self):
        """활동 시간 업데이트"""
        with self.activity_lock:
            self.last_activity_time = time.time()
    
    def watchdog(self):
        """5분 이상 활동 없으면 프로세스 중단"""
        while not self.should_stop:
            time.sleep(30)  # 30초마다 체크
            
            with self.activity_lock:
                inactive_time = time.time() - self.last_activity_time
            
            if inactive_time > 300:  # 5분 = 300초
                print(f"\n⚠️  5분 이상 비활성 감지 (비활성 시간: {inactive_time/60:.1f}분)")
                print("🔄 프로세스 재시작을 위해 현재 작업을 중단합니다...")
                self.should_stop = True
                os.kill(os.getpid(), signal.SIGTERM)
                break
    
    def start_watchdog(self):
        """워치독 시작"""
        self.watchdog_thread = threading.Thread(target=self.watchdog, daemon=True)
        self.watchdog_thread.start()
        print("🐕 워치독 시작: 5분 이상 비활성 시 자동 재시작")
    
    def load_cargo_list(self):
        """CSV 파일에서 화물 리스트 로드"""
        try:
            print("📄 cargolist.csv에서 화물 리스트 로드 중...")
            with open('cargolist.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                cargos = []
                for row in reader:
                    # ID_No, Guide_No, Name_of_Material 컬럼 사용
                    cargo_id = row['ID_No']
                    guide_no = row['Guide_No']
                    name = row['Name_of_Material']
                    
                    # ID가 없거나 "— —"인 경우 처리
                    if cargo_id and cargo_id != "— —":
                        if guide_no:
                            cargo_entry = f"{cargo_id}: {name} (Guide: {guide_no})"
                        else:
                            cargo_entry = f"{cargo_id}: {name}"
                    else:
                        # ID가 없는 경우 이름과 가이드만 사용
                        if guide_no:
                            cargo_entry = f"{name} (Guide: {guide_no})"
                        else:
                            cargo_entry = name
                    cargos.append(cargo_entry)
                print(f"📋 {len(cargos)}개 화물 로드됨")
                return cargos
        except FileNotFoundError:
            print("❌ cargolist.csv 파일을 찾을 수 없습니다.")
            return []
        except Exception as e:
            print(f"❌ CSV 로드 오류: {e}")
            return []
    
    def find_last_batch_number(self):
        """마지막 배치 번호 찾기"""
        timestamp_pattern = datetime.now().strftime("%Y%m%d")
        batch_files = list(Path('.').glob(f'maximum_data_batch_*_{timestamp_pattern}_*.csv'))
        
        if not batch_files:
            return 0
        
        # 파일명에서 배치 번호 추출
        batch_numbers = []
        for file in batch_files:
            try:
                # 파일명 형식: maximum_data_batch_X_YYYYMMDD_HHMM.csv
                parts = file.stem.split('_')
                batch_num = int(parts[3])  # X 부분
                batch_numbers.append(batch_num)
            except (IndexError, ValueError):
                continue
        
        return max(batch_numbers) if batch_numbers else 0
    
    def load_processed_cargos(self):
        """이미 처리된 화물들 로드"""
        timestamp_pattern = datetime.now().strftime("%Y%m%d")
        batch_files = list(Path('.').glob(f'maximum_data_batch_*_{timestamp_pattern}_*.csv'))
        
        processed = set()
        
        for file in batch_files:
            try:
                df = pd.read_csv(file)
                if not df.empty and 'Cargo' in df.columns:
                    processed.update(df['Cargo'].unique())
            except Exception as e:
                print(f"  ⚠️ 파일 읽기 오류 {file}: {e}")
        
        return processed
    
    def extract_stage_data(self, cargo, stage_name, prompt):
        """단계별 데이터 추출 (활동 시간 업데이트 포함)"""
        self.update_activity()
        
        try:
            print(f"    API 호출: {stage_name}...")
            response = model.generate_content(prompt)
            self.update_activity()
            
            # API 응답 안전 처리
            if hasattr(response, 'text') and response.text:
                return response.text
            elif hasattr(response, 'candidates') and response.candidates:
                if response.candidates[0].content.parts:
                    return response.candidates[0].content.parts[0].text
                else:
                    print(f"    ⚠️ {stage_name}: 응답 내용이 비어있음 (finish_reason: {response.candidates[0].finish_reason})")
                    # 대체 데이터 생성
                    return self.generate_fallback_data(cargo, stage_name)
            else:
                print(f"    ⚠️ {stage_name}: 예상치 못한 응답 형식")
                return f"응답 형식 오류 - {stage_name}"
                
        except Exception as e:
            print(f"    ⚠️ {stage_name} 오류: {e}")
            self.update_activity()
            return f"API 오류: {str(e)}"
    
    def generate_fallback_data(self, cargo, stage_name):
        """API 오류 시 대체 데이터 생성"""
        fallback_data = {
            "위험성 분석": [
                "ACUTE_INHALATION|Potential respiratory irritation from vapors|MODERATE|IMMEDIATE|HOURS|Direct lung exposure",
                "DERMAL_CONTACT|Skin irritation and potential absorption|MILD|MINUTES|DAYS|Direct skin contact",
                "EYE_EXPOSURE|Eye irritation and potential corneal damage|MODERATE|IMMEDIATE|HOURS|Direct eye contact",
                "ORAL_INGESTION|Gastrointestinal irritation if swallowed|MODERATE|MINUTES|HOURS|Accidental ingestion",
                "CHRONIC_EXPOSURE|Long-term health effects with repeated exposure|VARIABLE|MONTHS|PERMANENT|Occupational exposure"
            ],
            "응급처치": [
                "INHALATION_MILD|Remove to fresh air|Monitor breathing|Watch for respiratory distress|GOOD|Supportive care",
                "SKIN_CONTACT|Remove contaminated clothing, flush with water|Wash thoroughly|Monitor for irritation|GOOD|Prevent further exposure", 
                "EYE_CONTACT|Flush eyes with water for 15 minutes|Remove contact lenses|Seek medical attention|GOOD|Prevent corneal damage",
                "ORAL_INGESTION|Do not induce vomiting, rinse mouth|Give water if conscious|Seek immediate medical care|VARIABLE|Depends on amount",
                "SEVERE_EXPOSURE|Ensure airway, breathing, circulation|Provide oxygen if available|Evacuate immediately|GUARDED|Critical care needed"
            ],
            "통계 데이터": [
                "ACUTE_TOXICITY|LD50 data not readily available|Literature review|MEDIUM|General population|Further research needed",
                "EXPOSURE_LIMITS|Occupational exposure limits vary by jurisdiction|Regulatory agencies|HIGH|Workers|Compliance required",
                "EMERGENCY_CASES|Limited case reports available|Medical literature|LOW|Hospital records|Underreported",
                "RECOVERY_RATES|Most cases recover with prompt treatment|Clinical studies|MEDIUM|Exposed individuals|Good prognosis",
                "CHRONIC_EFFECTS|Long-term studies limited|Epidemiological data|LOW|Occupational cohorts|Ongoing research"
            ],
            "환경/추가 정보": [
                "ENVIRONMENTAL_FATE|Persistence and biodegradation data limited|Environmental studies|MEDIUM|Ecosystems|Monitoring needed",
                "REGULATORY_STATUS|Subject to various transportation regulations|Government agencies|HIGH|Industry compliance|Mandatory",
                "STORAGE_REQUIREMENTS|Store in appropriate containers and conditions|Safety guidelines|HIGH|Facility operators|Critical",
                "WASTE_DISPOSAL|Follow hazardous waste disposal protocols|Environmental regulations|HIGH|Waste handlers|Mandatory",
                "EMERGENCY_PLANNING|Include in emergency response procedures|Safety protocols|HIGH|Emergency responders|Essential"
            ],
            "선박 의약품 가이드라인": [
                "INHALATION_POISONING|아세트아미노펜 500mg, 생리식염주사액|경구/정맥주사|호흡수, 맥박 모니터|간질환시 금기|양호",
                "CHEMICAL_BURNS|베타딘액으로 소독, 화상가아제 적용|국소도포|상처 부위 관찰|감염 증상 주의|양호",
                "EYE_CONTAMINATION|생리식염주사액으로 15분간 세척|안구 세척|시력 변화 관찰|강제 개방 금지|양호",
                "SKIN_EXPOSURE|포비돈액 소독, 테라마이신연고 적용|국소도포|발적, 부종 관찰|알레르기 반응 주의|양호",
                "RESPIRATORY_DISTRESS|에피네프린 1앰플, 아미노필린|근육주사/정맥주사|호흡곤란 정도|심장질환시 주의|주의깊은 관찰"
            ]
        }
        
        stage_data = fallback_data.get(stage_name, ["GENERAL|Basic safety information|STANDARD|NORMAL|NONE|Consult medical professional"])
        return "\n".join(stage_data)
    
    def extract_maximum_data_stage1(self, cargo):
        """1단계: 위험성 분석"""
        prompt = f"""
        Analyze {cargo} comprehensively for ALL POSSIBLE health risks.
        
        PROVIDE EXACTLY 28-47 DETAILED HEALTH RISKS covering:
        1. ACUTE INHALATION (mild, moderate, severe, lethal concentrations)
        2. ACUTE DERMAL (splash, vapor, prolonged contact, sensitization)
        3. ACUTE OCULAR (direct contact, vapor, splash, permanent damage)
        4. ACUTE ORAL (accidental ingestion, intentional, various doses)
        5. CHRONIC INHALATION (occupational, environmental, long-term)
        6. CHRONIC DERMAL (repeated contact, absorption, accumulation)
        7. SYSTEMIC TOXICITY (liver, kidney, heart, lung, nervous system)
        8. REPRODUCTIVE EFFECTS (male, female, fertility, hormonal)
        9. DEVELOPMENTAL EFFECTS (pregnancy, fetal, pediatric, growth)
        10. CARCINOGENIC EFFECTS (various cancer types, mechanisms)
        11. RESPIRATORY SENSITIZATION (asthma, allergic reactions)
        12. SKIN SENSITIZATION (contact dermatitis, allergies)
        13. IMMUNOTOXICITY (immune suppression, autoimmune)
        14. NEUROTOXICITY (CNS, PNS, cognitive, motor effects)
        15. CARDIOVASCULAR EFFECTS (arrhythmia, hypertension, cardiac arrest)
        16. HEMATOLOGICAL EFFECTS (anemia, bleeding, blood chemistry)
        17. ENDOCRINE DISRUPTION (thyroid, adrenal, pancreatic)
        18. GENETIC TOXICITY (mutagenic, DNA damage, chromosomal)
        19. SPECIAL POPULATIONS (elderly, children, pregnant, diseased)
        20. ENVIRONMENTAL EXPOSURE (air, water, food contamination)
        
        Format: RISK_TYPE|DETAILED_DESCRIPTION|SEVERITY_LEVEL|ONSET_TIME|DURATION|MECHANISM
        MINIMUM 28 entries, MAXIMUM 47 entries. Be exhaustive and specific.
        """
        return self.extract_stage_data(cargo, "위험성 분석", prompt)
    
    def extract_maximum_data_stage2(self, cargo):
        """2단계: 응급처치"""
        prompt = f"""
        Comprehensive emergency procedures for {cargo}:
        
        PROVIDE EXACTLY 24-38 EMERGENCY SCENARIOS covering:
        1. MILD INHALATION (low concentration, brief exposure)
        2. MODERATE INHALATION (significant symptoms, respiratory distress)
        3. SEVERE INHALATION (life-threatening, pulmonary edema)
        4. SKIN CONTACT (liquid splash, minimal area)
        5. EXTENSIVE SKIN CONTACT (large area, prolonged exposure)
        6. EYE CONTACT (splash, vapor exposure, chemical burns)
        7. ORAL INGESTION (accidental small amounts)
        8. LARGE ORAL INGESTION (intentional, life-threatening)
        9. DERMAL ABSORPTION (chronic exposure through skin)
        10. COMBINED EXPOSURES (multiple routes simultaneously)
        11. PEDIATRIC EXPOSURES (children, dose adjustments)
        12. PREGNANCY EXPOSURES (maternal and fetal considerations)
        13. ELDERLY EXPOSURES (age-related complications)
        14. PRE-EXISTING CONDITIONS (asthma, liver disease, etc.)
        15. OCCUPATIONAL EXPOSURES (workplace accidents)
        16. MASS CASUALTY INCIDENTS (multiple victims)
        17. CONFINED SPACE EXPOSURES (enhanced toxicity)
        18. FIRE/EXPLOSION SCENARIOS (thermal injury + chemical)
        19. CONTAMINATED CLOTHING/PPE (decontamination procedures)
        20. TRANSPORTATION ACCIDENTS (spill response)
        21. HOME EXPOSURES (household accident management)
        22. DELAYED ONSET TOXICITY (symptoms hours/days later)
        23. ALLERGIC REACTIONS (sensitization responses)
        24. CHEMICAL BURNS (acid/base tissue damage)
        25. SYSTEMIC POISONING (organ failure management)
        
        Format: SCENARIO|IMMEDIATE_ACTION|MEDICAL_TREATMENT|MONITORING_REQUIREMENTS|PROGNOSIS|FOLLOW_UP
        MINIMUM 24 entries, MAXIMUM 38 entries. Include specific protocols.
        """
        return self.extract_stage_data(cargo, "응급처치", prompt)
    
    def extract_maximum_data_stage3(self, cargo):
        """3단계: 통계 데이터"""
        prompt = f"""
        Comprehensive statistical and epidemiological data for {cargo}:
        
        PROVIDE EXACTLY 19-33 DATA POINTS covering:
        1. ACUTE TOXICITY VALUES (LD50 oral, dermal, inhalation LC50)
        2. EXPOSURE LIMITS (PEL, TLV-TWA, TLV-STEL, TLV-C, IDLH)
        3. CARCINOGENICITY DATA (cancer slope factors, unit risk)
        4. DOSE-RESPONSE RELATIONSHIPS (NOAEL, LOAEL, BMD)
        5. HUMAN EPIDEMIOLOGY (occupational studies, case reports)
        6. MORTALITY STATISTICS (death rates, causes of death)
        7. MORBIDITY DATA (hospitalization rates, ICU admissions)
        8. RECOVERY STATISTICS (treatment success rates, disabilities)
        9. EMERGENCY ROOM VISITS (annual cases, severity distribution)
        10. LONG-TERM HEALTH OUTCOMES (chronic disease rates)
        11. PEDIATRIC EXPOSURE DATA (children's vulnerability factors)
        12. PREGNANCY OUTCOMES (birth defects, miscarriage rates)
        13. OCCUPATIONAL ILLNESS RATES (by industry, job function)
        14. ENVIRONMENTAL EXPOSURE LEVELS (air, water, soil concentrations)
        15. BIOMONITORING DATA (blood, urine levels in populations)
        16. PHARMACOKINETIC PARAMETERS (absorption, distribution, elimination)
        17. BENCHMARK DOSES (critical effect levels)
        18. RISK ASSESSMENT VALUES (cancer risk, non-cancer hazard)
        19. EMERGENCY RESPONSE STATISTICS (response times, outcomes)
        20. COST DATA (medical costs, lost productivity)
        21. REGULATORY STATUS (various countries, classifications)
        22. ANALYTICAL METHODS (detection limits, methods)
        23. ANIMAL STUDY DATA (species differences, extrapolation)
        24. MECHANISM OF ACTION DATA (molecular targets, pathways)
        25. GENETIC POLYMORPHISM EFFECTS (susceptible populations)
        
        Format: DATA_TYPE|SPECIFIC_VALUE|DATA_SOURCE|CONFIDENCE_LEVEL|POPULATION|NOTES
        MINIMUM 19 entries, MAXIMUM 33 entries. Include numerical values when available.
        """
        return self.extract_stage_data(cargo, "통계 데이터", prompt)
    
    def extract_maximum_data_stage4(self, cargo):
        """4단계: 환경/추가 정보"""
        prompt = f"""
        Comprehensive environmental, regulatory, and additional safety information for {cargo}:
        
        PROVIDE EXACTLY 19-28 INFORMATION ENTRIES covering:
        1. ENVIRONMENTAL FATE (biodegradation, persistence, half-life)
        2. ECOLOGICAL TOXICITY (aquatic, terrestrial, avian effects)
        3. BIOACCUMULATION POTENTIAL (BCF, BAF values)
        4. ENVIRONMENTAL MONITORING (detection in air, water, soil)
        5. CLIMATE CHANGE IMPACTS (ozone depletion, global warming)
        6. PHYSICAL PROPERTIES (density, viscosity, vapor pressure)
        7. CHEMICAL PROPERTIES (pH, reactivity, stability)
        8. FIRE/EXPLOSION HAZARDS (flash point, autoignition, LEL/UEL)
        9. INCOMPATIBLE MATERIALS (reaction hazards, forbidden mixtures)
        10. STORAGE REQUIREMENTS (temperature, humidity, container types)
        11. TRANSPORTATION REGULATIONS (UN classification, packaging)
        12. WORKPLACE REGULATIONS (OSHA, ACGIH standards)
        13. INTERNATIONAL REGULATIONS (EU REACH, GHS classification)
        14. RESTRICTED USE LISTS (banned countries, limited applications)
        15. SUBSTITUTION ALTERNATIVES (safer chemical options)
        16. GREEN CHEMISTRY ASPECTS (sustainable production, disposal)
        17. WASTE MANAGEMENT (treatment, disposal methods)
        18. EMERGENCY PLANNING (SARA Title III, RMP requirements)
        19. COMMUNITY RIGHT-TO-KNOW (reporting requirements)
        20. INDUSTRIAL HYGIENE (monitoring methods, PPE selection)
        21. RISK MANAGEMENT (process safety, prevention measures)
        22. TRAINING REQUIREMENTS (worker education, certification)
        23. MEDICAL SURVEILLANCE (health monitoring programs)
        24. INCIDENT REPORTING (mandatory reporting systems)
        25. INSURANCE CONSIDERATIONS (liability, coverage requirements)
        26. ECONOMIC IMPACTS (production costs, market trends)
        27. TECHNOLOGY DEVELOPMENTS (detection, treatment innovations)
        28. RESEARCH GAPS (unknown effects, needed studies)
        29. PUBLIC HEALTH IMPLICATIONS (community exposure risks)
        30. INTERNATIONAL COOPERATION (treaties, agreements)
        
        Format: CATEGORY|DETAILED_INFORMATION|REGULATORY_STATUS|IMPLEMENTATION_REQUIREMENTS|EFFECTIVENESS_DATA|REFERENCES
        MINIMUM 19 entries, MAXIMUM 28 entries. Provide specific details and references.
        """
        return self.extract_stage_data(cargo, "환경/추가 정보", prompt)
    
    def extract_maximum_data_stage5(self, cargo):
        """5단계: 선박 의약품 기반 응급 의학 가이드라인"""
        prompt = f"""
        You are a maritime medical expert analyzing cargo hazards for {cargo}.
        
        Available medicines on ship:
        {SHIP_MEDICINES}
        
        PROVIDE EXACTLY 15-25 MARITIME MEDICAL SCENARIOS covering:
        1. ACUTE INHALATION POISONING (mild, moderate, severe treatment protocols)
        2. CHEMICAL BURNS (acid, alkali, thermal injury management)
        3. EYE CONTAMINATION (irrigation, specific antidotes, vision protection)
        4. SKIN EXPOSURE (decontamination, wound care, systemic absorption)
        5. ORAL INGESTION (gastric lavage alternatives, activated charcoal, antidotes)
        6. RESPIRATORY DISTRESS (oxygen therapy, bronchodilators, ventilation)
        7. CARDIOVASCULAR COLLAPSE (shock management, cardiac support)
        8. NEUROLOGICAL SYMPTOMS (seizures, coma, altered consciousness)
        9. ALLERGIC REACTIONS (anaphylaxis, sensitization responses)
        10. SYSTEMIC TOXICITY (liver, kidney, multi-organ support)
        11. BURN WOUND MANAGEMENT (cooling, dressing, pain control)
        12. INFECTION PREVENTION (wound care, antibiotic prophylaxis)
        13. PAIN MANAGEMENT (analgesics available on ship)
        14. SHOCK TREATMENT (fluid resuscitation, vasopressors)
        15. ANTIDOTE ADMINISTRATION (specific reversal agents if available)
        16. SUPPORTIVE CARE (IV fluids, electrolyte management)
        17. EVACUATION PROTOCOLS (stabilization for transfer)
        18. MONITORING REQUIREMENTS (vital signs, laboratory alternatives)
        19. DRUG INTERACTIONS (maritime medicine compatibility)
        20. DOSE CALCULATIONS (weight-based, age-adjusted dosing)
        21. ADMINISTRATION ROUTES (IV, IM, PO, topical preferences)
        22. CONTRAINDICATIONS (when NOT to use specific medicines)
        23. EMERGENCY PROCEDURES (when medicines are insufficient)
        24. PREVENTION MEASURES (post-exposure prophylaxis)
        25. COMMUNICATION PROTOCOLS (medical advice via radio)
        
        Format: MEDICAL_SCENARIO|SPECIFIC_SHIP_MEDICINES|DOSAGE_ROUTE|MONITORING|CONTRAINDICATIONS|PROGNOSIS
        MINIMUM 15 entries, MAXIMUM 25 entries.
        
        CRITICAL: For each scenario, you MUST:
        - Use ONLY medicines from the ship's inventory listed above
        - Specify exact medicine names (e.g., "에피네프린 1앰플", "아세트아미노펜 500mg")
        - Include dosage and administration route
        - Consider maritime environment limitations
        - Provide realistic treatment protocols using available resources
        """
        return self.extract_stage_data(cargo, "선박 의약품 가이드라인", prompt)
    
    
    def parse_stage_data(self, cargo, stage_data, stage_name):
        """데이터 파싱"""
        results = []
        
        if not stage_data:
            return results
        
        lines = stage_data.split('\n')
        for line in lines:
            line = line.strip()
            if '|' in line and len(line) > 20:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 3:
                    results.append({
                        'Cargo': cargo,
                        'Stage': stage_name,
                        'Category': parts[0] if len(parts) > 0 else '',
                        'Description': parts[1] if len(parts) > 1 else '',
                        'Detail1': parts[2] if len(parts) > 2 else '',
                        'Detail2': parts[3] if len(parts) > 3 else '',
                        'Detail3': parts[4] if len(parts) > 4 else ''
                    })
        
        # 백업 파싱
        if not results:
            sentences = [s.strip() for s in stage_data.replace('\n', '. ').split('.')]
            relevant_sentences = [s for s in sentences if len(s) > 30 and 
                                any(keyword in s.lower() for keyword in 
                                ['risk', 'toxic', 'hazard', 'treat', 'procedure', 'data', 'effect'])]
            
            for i, sentence in enumerate(relevant_sentences[:10]):
                results.append({
                    'Cargo': cargo,
                    'Stage': stage_name,
                    'Category': f'{stage_name} Item {i+1}',
                    'Description': sentence,
                    'Detail1': '',
                    'Detail2': '',
                    'Detail3': ''
                })
        
        return results
    
    def analyze_cargo_maximum(self, cargo, cargo_num, total_cargos):
        """단일 화물 분석"""
        if self.should_stop:
            return []
        
        print(f"\n[{cargo_num}/{total_cargos}] {cargo}")
        self.update_activity()
        
        all_results = []
        
        # 5단계 분석
        stages = [
            ("위험성 분석", self.extract_maximum_data_stage1, "Risk Analysis"),
            ("응급처치", self.extract_maximum_data_stage2, "Emergency Procedures"),
            ("통계 데이터", self.extract_maximum_data_stage3, "Statistical Data"),
            ("환경/추가 정보", self.extract_maximum_data_stage4, "Environmental/Additional"),
            ("선박 의약품 가이드라인", self.extract_maximum_data_stage5, "Maritime Medical Guidelines")
        ]
        
        for stage_name, stage_func, stage_key in stages:
            if self.should_stop:
                break
                
            print(f"  Stage: {stage_name}...")
            stage_data = stage_func(cargo)
            stage_results = self.parse_stage_data(cargo, stage_data, stage_key)
            all_results.extend(stage_results)
            print(f"    ✓ {len(stage_results)}개 항목")
            
            time.sleep(1)  # API 제한 고려
            self.update_activity()
        
        print(f"  🎯 총 {len(all_results)}개 데이터 항목")
        return all_results
    
    def save_batch_results(self, results, batch_num):
        """배치 결과 저장"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"maximum_data_batch_{batch_num}_{timestamp}.csv"
        
        df = pd.DataFrame(results)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        self.update_activity()
        return filename
    
    def run_analysis(self):
        """분석 실행"""
        print("="*100)
        print("🔄 AUTO-RESTART MAXIMUM DATA EXTRACTION")
        print("="*100)
        
        # 이전 진행 상황 확인
        last_batch = self.find_last_batch_number()
        processed_cargos = self.load_processed_cargos()
        
        print(f"📋 재시작 정보:")
        print(f"  - 마지막 배치 번호: {last_batch}")
        print(f"  - 처리된 화물 수: {len(processed_cargos)}개")
        
        # 화물 리스트 로드
        all_cargos = self.load_cargo_list()
        
        # 미처리 화물만 필터링
        remaining_cargos = [cargo for cargo in all_cargos if cargo not in processed_cargos]
        
        print(f"  - 남은 화물 수: {len(remaining_cargos)}개")
        print(f"  - 전체 진행률: {len(processed_cargos)}/{len(all_cargos)} ({len(processed_cargos)/len(all_cargos)*100:.1f}%)")
        
        if not remaining_cargos:
            print("✅ 모든 화물 처리 완료!")
            return
        
        # 워치독 시작
        self.start_watchdog()
        
        # 분석 시작
        start_time = time.time()
        batch_size = 10
        self.current_batch = last_batch
        
        print(f"\n🚀 분석 재시작... ({datetime.now().strftime('%H:%M:%S')})")
        print("-"*100)
        
        for i in range(0, len(remaining_cargos), batch_size):
            if self.should_stop:
                print("\n🔄 워치독에 의해 중단됨")
                break
                
            batch_cargos = remaining_cargos[i:i+batch_size]
            self.current_batch += 1
            
            print(f"\n📦 BATCH {self.current_batch} ({len(batch_cargos)}개 화물)")
            print("="*50)
            
            batch_results = []
            
            for j, cargo in enumerate(batch_cargos):
                if self.should_stop:
                    break
                    
                cargo_results = self.analyze_cargo_maximum(cargo, i+j+1, len(remaining_cargos))
                batch_results.extend(cargo_results)
                self.processed_cargos.add(cargo)
            
            # 배치 저장
            if batch_results:
                batch_file = self.save_batch_results(batch_results, self.current_batch)
                print(f"\n💾 배치 {self.current_batch} 저장: {batch_file}")
                print(f"   배치 데이터: {len(batch_results)}개")
                
                # 진행률 표시
                total_processed = len(processed_cargos) + i + len(batch_cargos)
                progress = (total_processed / len(all_cargos)) * 100
                elapsed = (time.time() - start_time) / 60
                print(f"   전체 진행률: {progress:.1f}% | 경과: {elapsed:.1f}분")
        
        self.should_stop = True
        
        if not self.should_stop:
            print(f"\n🎉 분석 완료!")
        else:
            print(f"\n⏸️  분석 일시 중단 (재시작 가능)")

def main():
    def signal_handler(signum, frame):
        print(f"\n💡 신호 {signum} 받음 - 정상 종료 중...")
        exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    analyzer = AutoRestartAnalyzer()
    
    try:
        analyzer.run_analysis()
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단됨")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        print("🔄 재시작하면 중단된 지점부터 계속됩니다")

if __name__ == "__main__":
    main()