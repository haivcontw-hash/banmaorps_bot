const SUPPORTED_LANGS = ['en', 'vi', 'zh', 'ru', 'ko', 'id'];

const SCIENCE_TEMPLATES = {
    physics: {
        en: 'Physics: Which formula describes {concept}?',
        vi: 'Vật lý: Công thức nào mô tả {concept}?',
        zh: '物理：哪个公式描述了{concept}？',
        ru: 'Физика: какая формула описывает {concept}?',
        ko: '물리: 어떤 공식이 {concept}을/를 설명하나요?',
        id: 'Fisika: Rumus mana yang menggambarkan {concept}?'
    },
    chemistry: {
        en: 'Chemistry: Which formula represents {concept}?',
        vi: 'Hóa học: Công thức nào biểu diễn {concept}?',
        zh: '化学：哪个公式表示{concept}？',
        ru: 'Химия: какая формула описывает {concept}?',
        ko: '화학: 어떤 식이 {concept}을/를 나타내나요?',
        id: 'Kimia: Rumus mana yang mewakili {concept}?'
    },
    okx: {
        en: 'OKX quiz: {concept}',
        vi: 'Câu hỏi OKX: {concept}',
        zh: 'OKX问答：{concept}',
        ru: 'Викторина OKX: {concept}',
        ko: 'OKX 퀴즈: {concept}',
        id: 'Kuis OKX: {concept}'
    },
    web3: {
        en: 'Web3 quiz: {concept}',
        vi: 'Câu hỏi Web3: {concept}',
        zh: 'Web3问答：{concept}',
        ru: 'Викторина Web3: {concept}',
        ko: 'Web3 퀴즈: {concept}',
        id: 'Kuis Web3: {concept}'
    }
};

function normalizeConcept(concept) {
    const normalized = {};
    for (const lang of SUPPORTED_LANGS) {
        normalized[lang] = concept[lang] || concept.en;
    }
    return normalized;
}

function makeEntry(concept, formula) {
    return {
        concept: normalizeConcept(concept),
        formula
    };
}

const physicsConcepts = [
    makeEntry({
        en: "Newton's second law",
        vi: 'định luật II Newton',
        zh: '牛顿第二定律',
        ru: 'второй закон Ньютона',
        ko: '뉴턴의 제2법칙',
        id: 'hukum kedua Newton'
    }, 'F = m × a'),
    makeEntry({
        en: 'linear momentum definition',
        vi: 'định nghĩa động lượng tuyến tính',
        zh: '线动量的定义',
        ru: 'определение линейного импульса',
        ko: '선운동량의 정의',
        id: 'definisi momentum linear'
    }, 'p = m × v'),
    makeEntry({
        en: 'kinetic energy of a moving mass',
        vi: 'động năng của vật chuyển động',
        zh: '运动物体的动能',
        ru: 'кинетическая энергия движущейся массы',
        ko: '움직이는 질량의 운동 에너지',
        id: 'energi kinetik massa bergerak'
    }, 'E_k = 1/2 × m × v^2'),
    makeEntry({
        en: 'gravitational potential energy near Earth',
        vi: 'thế năng hấp dẫn gần bề mặt Trái Đất',
        zh: '地表附近的引力势能',
        ru: 'гравитационная потенциальная энергия у поверхности Земли',
        ko: '지구 표면 근처의 중력 위치 에너지',
        id: 'energi potensial gravitasi dekat permukaan Bumi'
    }, 'U = m × g × h'),
    makeEntry({
        en: "Hooke's law",
        vi: 'định luật Hooke',
        zh: '胡克定律',
        ru: 'закон Гука',
        ko: '훅의 법칙',
        id: 'hukum Hooke'
    }, 'F = k × x'),
    makeEntry({
        en: "Ohm's law",
        vi: 'định luật Ohm',
        zh: '欧姆定律',
        ru: 'закон Ома',
        ko: '옴의 법칙',
        id: 'hukum Ohm'
    }, 'V = I × R'),
    makeEntry({
        en: 'electric power relationship',
        vi: 'quan hệ công suất điện',
        zh: '电功率关系',
        ru: 'соотношение электрической мощности',
        ko: '전력 관계식',
        id: 'hubungan daya listrik'
    }, 'P = I × V'),
    makeEntry({
        en: 'wave speed equation',
        vi: 'phương trình vận tốc sóng',
        zh: '波速公式',
        ru: 'формула скорости волны',
        ko: '파동 속도 공식',
        id: 'persamaan kecepatan gelombang'
    }, 'v = f × λ'),
    makeEntry({
        en: "Snell's law of refraction",
        vi: 'định luật khúc xạ Snell',
        zh: '斯涅尔折射定律',
        ru: 'закон преломления Снелла',
        ko: '스넬의 굴절 법칙',
        id: 'hukum pembiasan Snell'
    }, 'n₁ × sinθ₁ = n₂ × sinθ₂'),
    makeEntry({
        en: "Coulomb's law",
        vi: 'định luật Coulomb',
        zh: '库仑定律',
        ru: 'закон Кулона',
        ko: '쿨롱의 법칙',
        id: 'hukum Coulomb'
    }, 'F = k × q₁ × q₂ / r^2'),
    makeEntry({
        en: 'electric field strength definition',
        vi: 'định nghĩa cường độ điện trường',
        zh: '电场强度的定义',
        ru: 'определение напряженности электрического поля',
        ko: '전기장 세기의 정의',
        id: 'definisi kuat medan listrik'
    }, 'E = F / q'),
    makeEntry({
        en: 'capacitance definition',
        vi: 'định nghĩa điện dung',
        zh: '电容的定义',
        ru: 'определение емкости',
        ko: '정전용량의 정의',
        id: 'definisi kapasitansi'
    }, 'C = Q / V'),
    makeEntry({
        en: 'energy stored in a capacitor',
        vi: 'năng lượng trong tụ điện',
        zh: '电容中储存的能量',
        ru: 'энергия, запасенная в конденсаторе',
        ko: '커패시터에 저장된 에너지',
        id: 'energi yang tersimpan dalam kapasitor'
    }, 'U = 1/2 × C × V^2'),
    makeEntry({
        en: 'magnetic force on a current-carrying wire',
        vi: 'lực từ trên dây dẫn có dòng điện',
        zh: '载流导线的磁力',
        ru: 'магнитная сила на проводнике с током',
        ko: '전류가 흐르는 도선에 작용하는 자기력',
        id: 'gaya magnet pada kawat berarus'
    }, 'F = I × L × B × sinθ'),
    makeEntry({
        en: 'Lorentz force on a moving charge',
        vi: 'lực Lorentz lên hạt mang điện',
        zh: '运动电荷的洛伦兹力',
        ru: 'сила Лоренца на движущийся заряд',
        ko: '움직이는 전하에 작용하는 로런츠 힘',
        id: 'gaya Lorentz pada muatan bergerak'
    }, 'F = q × v × B × sinθ'),
    makeEntry({
        en: 'angular momentum of a particle',
        vi: 'mô men động lượng của hạt',
        zh: '粒子的角动量',
        ru: 'угловой момент частицы',
        ko: '입자의 각운동량',
        id: 'momentum sudut partikel'
    }, 'L = r × p'),
    makeEntry({
        en: 'centripetal acceleration',
        vi: 'gia tốc hướng tâm',
        zh: '向心加速度',
        ru: 'центростремительное ускорение',
        ko: '구심가속도',
        id: 'percepatan sentripetal'
    }, 'a_c = v^2 / r'),
    makeEntry({
        en: 'centripetal force requirement',
        vi: 'lực hướng tâm cần thiết',
        zh: '所需向心力',
        ru: 'необходимая центростремительная сила',
        ko: '필요한 구심력',
        id: 'gaya sentripetal yang dibutuhkan'
    }, 'F_c = m × v^2 / r'),
    makeEntry({
        en: 'period of a simple pendulum',
        vi: 'chu kỳ con lắc đơn',
        zh: '单摆周期',
        ru: 'период математического маятника',
        ko: '단순 진자의 주기',
        id: 'periode bandul sederhana'
    }, 'T = 2π × √(L / g)'),
    makeEntry({
        en: 'escape velocity from a planet',
        vi: 'vận tốc thoát khỏi hành tinh',
        zh: '行星逃逸速度',
        ru: 'вторая космическая скорость',
        ko: '행성 탈출 속도',
        id: 'kecepatan lepas dari planet'
    }, 'v_e = √(2 × G × M / r)'),
    makeEntry({
        en: "Einstein's mass–energy equivalence",
        vi: 'nguyên lý tương đương khối lượng - năng lượng của Einstein',
        zh: '爱因斯坦质能方程',
        ru: 'эквивалентность массы и энергии Эйнштейна',
        ko: '아인슈타인의 질량-에너지 등가',
        id: 'kesetaraan massa-energi Einstein'
    }, 'E = m × c^2'),
    makeEntry({
        en: 'photon energy from frequency',
        vi: 'năng lượng photon theo tần số',
        zh: '光子的能量与频率',
        ru: 'энергия фотона через частоту',
        ko: '주파수에 따른 광자 에너지',
        id: 'energi foton dari frekuensi'
    }, 'E = h × f'),
    makeEntry({
        en: 'de Broglie wavelength',
        vi: 'bước sóng de Broglie',
        zh: '德布罗意波长',
        ru: 'длина волны де Бройля',
        ko: '드브로이 파장',
        id: 'panjang gelombang de Broglie'
    }, 'λ = h / p'),
    makeEntry({
        en: 'pressure definition',
        vi: 'định nghĩa áp suất',
        zh: '压强的定义',
        ru: 'определение давления',
        ko: '압력의 정의',
        id: 'definisi tekanan'
    }, 'P = F / A'),
    makeEntry({
        en: 'density calculation',
        vi: 'công thức tính khối lượng riêng',
        zh: '密度计算',
        ru: 'расчет плотности',
        ko: '밀도 계산식',
        id: 'perhitungan massa jenis'
    }, 'ρ = m / V'),
    makeEntry({
        en: 'work done by a constant force',
        vi: 'công thực hiện bởi lực không đổi',
        zh: '恒力所做的功',
        ru: 'работа постоянной силы',
        ko: '일정한 힘이 한 일',
        id: 'usaha oleh gaya konstan'
    }, 'W = F × d × cosθ'),
    makeEntry({
        en: 'impulse-momentum theorem',
        vi: 'định lý xung lượng-động lượng',
        zh: '冲量动量定理',
        ru: 'теорема импульса и импульса',
        ko: '충격량-운동량 정리',
        id: 'teorema impuls-momentum'
    }, 'J = F × Δt = Δp'),
    makeEntry({
        en: 'ideal gas law',
        vi: 'phương trình khí lý tưởng',
        zh: '理想气体状态方程',
        ru: 'уравнение состояния идеального газа',
        ko: '이상 기체 상태 방정식',
        id: 'hukum gas ideal'
    }, 'P × V = n × R × T'),
    makeEntry({
        en: "Boyle's law",
        vi: 'định luật Boyle',
        zh: '波义耳定律',
        ru: 'закон Бойля',
        ko: '보일의 법칙',
        id: 'hukum Boyle'
    }, 'P₁ × V₁ = P₂ × V₂'),
    makeEntry({
        en: "Charles's law",
        vi: 'định luật Charles',
        zh: '查理定律',
        ru: 'закон Шарля',
        ko: '샤를의 법칙',
        id: 'hukum Charles'
    }, 'V₁ / T₁ = V₂ / T₂'),
    makeEntry({
        en: 'Bernoulli equation for fluid flow',
        vi: 'phương trình Bernoulli cho dòng chảy',
        zh: '流体伯努利方程',
        ru: 'уравнение Бернулли для потока',
        ko: '유체 흐름의 베르누이 방정식',
        id: 'persamaan Bernoulli untuk aliran fluida'
    }, 'P + 1/2 × ρ × v^2 + ρ × g × h = const'),
    makeEntry({
        en: 'continuity equation for incompressible fluid',
        vi: 'phương trình liên tục cho chất lỏng không nén được',
        zh: '不可压缩流体的连续性方程',
        ru: 'уравнение неразрывности для несжимаемой жидкости',
        ko: '비압축성 유체의 연속 방정식',
        id: 'persamaan kontinuitas fluida tak termampatkan'
    }, 'A₁ × v₁ = A₂ × v₂'),
    makeEntry({
        en: 'specific heat capacity formula',
        vi: 'công thức nhiệt dung riêng',
        zh: '比热容公式',
        ru: 'формула удельной теплоемкости',
        ko: '비열 공식',
        id: 'rumus kalor jenis'
    }, 'Q = m × c × ΔT'),
    makeEntry({
        en: 'linear thermal expansion',
        vi: 'nở dài vì nhiệt',
        zh: '线膨胀',
        ru: 'линейное тепловое расширение',
        ko: '선팽창',
        id: 'muai panjang linear'
    }, 'ΔL = α × L₀ × ΔT'),
    makeEntry({
        en: 'Stefan–Boltzmann law',
        vi: 'định luật Stefan–Boltzmann',
        zh: '斯特藩-玻尔兹曼定律',
        ru: 'закон Стефана — Больцмана',
        ko: '스테판-볼츠만 법칙',
        id: 'hukum Stefan–Boltzmann'
    }, 'P = σ × A × T^4'),
    makeEntry({
        en: "Wien's displacement law",
        vi: 'định luật dịch chuyển Wien',
        zh: '维恩位移定律',
        ru: 'закон смещения Вина',
        ko: '빈의 이동 법칙',
        id: 'hukum perpindahan Wien'
    }, 'λ_max × T = b'),
    makeEntry({
        en: 'mirror equation',
        vi: 'phương trình gương',
        zh: '镜面方程',
        ru: 'зеркальное уравнение',
        ko: '거울 방정식',
        id: 'persamaan cermin'
    }, '1/f = 1/d_o + 1/d_i'),
    makeEntry({
        en: 'magnification formula',
        vi: 'công thức độ phóng đại',
        zh: '放大率公式',
        ru: 'формула увеличения',
        ko: '배율 공식',
        id: 'rumus perbesaran'
    }, 'm = -d_i / d_o'),
    makeEntry({
        en: 'electric potential energy between charges',
        vi: 'thế năng điện giữa hai điện tích',
        zh: '电荷之间的电势能',
        ru: 'потенциальная энергия между зарядами',
        ko: '전하 사이의 전기 위치 에너지',
        id: 'energi potensial listrik antara muatan'
    }, 'U = k × q₁ × q₂ / r'),
    makeEntry({
        en: 'Newtonian gravitation',
        vi: 'định luật hấp dẫn Newton',
        zh: '牛顿万有引力定律',
        ru: 'закон всемирного тяготения Ньютона',
        ko: '뉴턴의 만유인력 법칙',
        id: 'hukum gravitasi Newton'
    }, 'F = G × m₁ × m₂ / r^2'),
    makeEntry({
        en: 'orbital velocity for circular orbit',
        vi: 'vận tốc quỹ đạo tròn',
        zh: '圆形轨道的轨道速度',
        ru: 'орбитальная скорость на круговой орбите',
        ko: '원 궤도 속도',
        id: 'kecepatan orbit melingkar'
    }, 'v = √(G × M / r)'),
    makeEntry({
        en: 'Kepler-inspired orbital period',
        vi: 'chu kỳ quỹ đạo kiểu Kepler',
        zh: '开普勒形式的轨道周期',
        ru: 'орбитальный период по Кеплеру',
        ko: '케플러식 공전 주기',
        id: 'periode orbit ala Kepler'
    }, 'T = 2π × √(r^3 / (G × M))'),
    makeEntry({
        en: 'torque definition',
        vi: 'định nghĩa mô men lực',
        zh: '力矩的定义',
        ru: 'определение крутящего момента',
        ko: '토크의 정의',
        id: 'definisi torsi'
    }, 'τ = r × F × sinθ'),
    makeEntry({
        en: 'rotational kinetic energy',
        vi: 'động năng quay',
        zh: '转动动能',
        ru: 'вращательная кинетическая энергия',
        ko: '회전 운동 에너지',
        id: 'energi kinetik rotasi'
    }, 'E_rot = 1/2 × I × ω^2'),
    makeEntry({
        en: 'rotational form of Newton’s second law',
        vi: 'dạng quay của định luật II Newton',
        zh: '牛顿第二定律的转动形式',
        ru: 'вращательная форма второго закона Ньютона',
        ko: '뉴턴 제2법칙의 회전 형태',
        id: 'bentuk rotasi hukum II Newton'
    }, 'τ = I × α'),
    makeEntry({
        en: 'resistance from resistivity',
        vi: 'điện trở từ điện trở suất',
        zh: '由电阻率求电阻',
        ru: 'сопротивление через удельное сопротивление',
        ko: '저항률로 구한 저항',
        id: 'hambatan dari resistivitas'
    }, 'R = ρ × L / A'),
    makeEntry({
        en: 'drift velocity relation',
        vi: 'quan hệ vận tốc trôi',
        zh: '漂移速度关系式',
        ru: 'соотношение дрейфовой скорости',
        ko: '드리프트 속도 관계식',
        id: 'hubungan kecepatan drift'
    }, 'I = n × q × A × v_d'),
    makeEntry({
        en: 'magnetic flux definition',
        vi: 'định nghĩa từ thông',
        zh: '磁通量定义',
        ru: 'определение магнитного потока',
        ko: '자속의 정의',
        id: 'definisi fluks magnet'
    }, 'Φ_B = B × A × cosθ'),
    makeEntry({
        en: "Faraday's law of induction",
        vi: 'định luật cảm ứng Faraday',
        zh: '法拉第电磁感应定律',
        ru: 'закон электромагнитной индукции Фарадея',
        ko: '패러데이 유도 법칙',
        id: 'hukum induksi Faraday'
    }, 'ε = -N × dΦ_B / dt'),
    makeEntry({
        en: 'inductive reactance',
        vi: 'cảm kháng',
        zh: '感抗',
        ru: 'индуктивное сопротивление',
        ko: '유도 리액턴스',
        id: 'reaktansi induktif'
    }, 'X_L = 2π × f × L'),
    makeEntry({
        en: 'capacitive reactance',
        vi: 'dung kháng',
        zh: '容抗',
        ru: 'емкостное сопротивление',
        ko: '용량 리액턴스',
        id: 'reaktansi kapasitif'
    }, 'X_C = 1 / (2π × f × C)'),
    makeEntry({
        en: 'root-mean-square voltage',
        vi: 'điện áp hiệu dụng',
        zh: '有效电压',
        ru: 'действующее значение напряжения',
        ko: '실효 전압',
        id: 'tegangan RMS'
    }, 'V_rms = V_max / √2'),
    makeEntry({
        en: 'root-mean-square current',
        vi: 'dòng điện hiệu dụng',
        zh: '有效电流',
        ru: 'действующее значение тока',
        ko: '실효 전류',
        id: 'arus RMS'
    }, 'I_rms = I_max / √2'),
    makeEntry({
        en: 'sound intensity level',
        vi: 'mức cường độ âm',
        zh: '声强级',
        ru: 'уровень звуковой интенсивности',
        ko: '음 강도 레벨',
        id: 'tingkat intensitas bunyi'
    }, 'β = 10 × log₁₀(I / I₀)'),
    makeEntry({
        en: 'Doppler effect for moving source and observer',
        vi: 'hiệu ứng Doppler khi nguồn và người quan sát chuyển động',
        zh: '源与观察者运动时的多普勒效应',
        ru: 'эффект Доплера при движении источника и наблюдателя',
        ko: '원천과 관측자가 움직일 때 도플러 효과',
        id: 'efek Doppler saat sumber dan pengamat bergerak'
    }, "f' = f × (v ± v_o) / (v ∓ v_s)"),
    makeEntry({
        en: 'time dilation in special relativity',
        vi: 'giãn thời gian trong thuyết tương đối hẹp',
        zh: '狭义相对论中的时间膨胀',
        ru: 'замедление времени в СТО',
        ko: '특수상대론의 시간 지연',
        id: 'dilatasi waktu dalam relativitas khusus'
    }, "Δt' = γ × Δt"),
    makeEntry({
        en: 'Lorentz factor definition',
        vi: 'định nghĩa hệ số Lorentz',
        zh: '洛伦兹因子的定义',
        ru: 'определение лоренц-фактора',
        ko: '로런츠 팩터 정의',
        id: 'definisi faktor Lorentz'
    }, 'γ = 1 / √(1 - v^2 / c^2)'),
    makeEntry({
        en: 'length contraction in relativity',
        vi: 'co độ dài trong thuyết tương đối',
        zh: '相对论中的长度收缩',
        ru: 'сокращение длины в теории относительности',
        ko: '상대론적 길이 수축',
        id: 'kontraksi panjang relativistik'
    }, 'L = L₀ / γ'),
    makeEntry({
        en: 'photoelectric effect energy balance',
        vi: 'cân bằng năng lượng trong hiệu ứng quang điện',
        zh: '光电效应的能量平衡',
        ru: 'баланс энергии в фотоэффекте',
        ko: '광전 효과의 에너지 균형',
        id: 'neraca energi efek fotolistrik'
    }, 'h × f = Φ + K_max'),
    makeEntry({
        en: 'Joule heating in a resistor',
        vi: 'nhiệt lượng Joule trong điện trở',
        zh: '电阻焦耳热',
        ru: 'джоулево тепло в резисторе',
        ko: '저항에서의 줄 열',
        id: 'pemanasan Joule pada resistor'
    }, 'P = I^2 × R'),
    makeEntry({
        en: 'stress definition',
        vi: 'định nghĩa ứng suất',
        zh: '应力定义',
        ru: 'определение напряжения',
        ko: '응력의 정의',
        id: 'definisi tegangan'
    }, 'σ = F / A'),
    makeEntry({
        en: 'strain definition',
        vi: 'định nghĩa biến dạng',
        zh: '应变定义',
        ru: 'определение деформации',
        ko: '변형률의 정의',
        id: 'definisi regangan'
    }, 'ε = ΔL / L₀'),
    makeEntry({
        en: "Young's modulus",
        vi: 'môđun Young',
        zh: '杨氏模量',
        ru: 'модуль Юнга',
        ko: '영률',
        id: 'modulus Young'
    }, 'E = σ / ε'),
    makeEntry({
        en: "Poisson's ratio",
        vi: 'hệ số Poisson',
        zh: '泊松比',
        ru: 'коэффициент Пуассона',
        ko: '푸아송 비',
        id: 'rasio Poisson'
    }, 'ν = -ε_trans / ε_axial'),
    makeEntry({
        en: 'bulk modulus',
        vi: 'môđun nén',
        zh: '体弹模量',
        ru: 'объемный модуль упругости',
        ko: '체적 탄성률',
        id: 'modulus curah'
    }, 'B = -ΔP / (ΔV / V)'),
    makeEntry({
        en: 'shear modulus',
        vi: 'môđun trượt',
        zh: '剪切模量',
        ru: 'модуль сдвига',
        ko: '전단 탄성률',
        id: 'modulus geser'
    }, 'G = τ / γ'),
    makeEntry({
        en: 'thermal conduction rate',
        vi: 'tốc độ dẫn nhiệt',
        zh: '热传导速率',
        ru: 'скорость теплопередачи',
        ko: '열전도 속도',
        id: 'laju konduksi panas'
    }, 'Q̇ = k × A × ΔT / L'),
    makeEntry({
        en: 'work-energy theorem',
        vi: 'định lý công - năng lượng',
        zh: '功能定理',
        ru: 'теорема работы и энергии',
        ko: '일-에너지 정리',
        id: 'teorema kerja-energi'
    }, 'W_net = ΔK'),
    makeEntry({
        en: 'power definition',
        vi: 'định nghĩa công suất',
        zh: '功率的定义',
        ru: 'определение мощности',
        ko: '전력의 정의',
        id: 'definisi daya'
    }, 'P = W / t'),
    makeEntry({
        en: 'frequency-period relationship',
        vi: 'quan hệ giữa tần số và chu kỳ',
        zh: '频率与周期的关系',
        ru: 'связь между частотой и периодом',
        ko: '주파수와 주기의 관계',
        id: 'hubungan frekuensi dan periode'
    }, 'f = 1 / T'),
    makeEntry({
        en: 'point source intensity drop',
        vi: 'suy giảm cường độ nguồn điểm',
        zh: '点源强度衰减',
        ru: 'ослабление интенсивности точечного источника',
        ko: '점광원 세기 감소',
        id: 'penurunan intensitas sumber titik'
    }, 'I = P / (4π × r^2)'),
    makeEntry({
        en: 'specific latent heat',
        vi: 'nhiệt ẩn riêng',
        zh: '比潜热',
        ru: 'удельная скрытая теплота',
        ko: '비잠열',
        id: 'kalor laten spesifik'
    }, 'Q = m × L'),
    makeEntry({
        en: 'average kinetic energy of gas particles',
        vi: 'động năng trung bình của hạt khí',
        zh: '气体分子的平均动能',
        ru: 'средняя кинетическая энергия частиц газа',
        ko: '기체 입자의 평균 운동 에너지',
        id: 'energi kinetik rata-rata partikel gas'
    }, 'E_avg = 3/2 × k_B × T'),
    makeEntry({
        en: 'root-mean-square speed of gas',
        vi: 'vận tốc hiệu dụng của khí',
        zh: '气体的均方根速率',
        ru: 'среднеквадратичная скорость газа',
        ko: '기체의 RMS 속도',
        id: 'kecepatan RMS gas'
    }, 'v_rms = √(3 × k_B × T / m)'),
    makeEntry({
        en: 'heat engine efficiency',
        vi: 'hiệu suất động cơ nhiệt',
        zh: '热机效率',
        ru: 'КПД теплового двигателя',
        ko: '열기관 효율',
        id: 'efisiensi mesin kalor'
    }, 'η = 1 - Q_c / Q_h'),
    makeEntry({
        en: 'Carnot efficiency',
        vi: 'hiệu suất Carnot',
        zh: '卡诺效率',
        ru: 'эффективность цикла Карно',
        ko: '카르노 효율',
        id: 'efisiensi Carnot'
    }, 'η = 1 - T_c / T_h'),
    makeEntry({
        en: 'charge stored on a capacitor',
        vi: 'điện tích trên tụ điện',
        zh: '电容器上的电荷',
        ru: 'заряд на конденсаторе',
        ko: '커패시터에 저장된 전하',
        id: 'muatan pada kapasitor'
    }, 'Q = C × V'),
    makeEntry({
        en: 'electric potential difference',
        vi: 'hiệu điện thế',
        zh: '电势差',
        ru: 'разность электрических потенциалов',
        ko: '전위차',
        id: 'beda potensial listrik'
    }, 'V = W / q'),
    makeEntry({
        en: 'parallel resistance combination',
        vi: 'điện trở song song',
        zh: '并联电阻',
        ru: 'параллельное соединение резисторов',
        ko: '병렬 저항 결합',
        id: 'kombinasi resistansi paralel'
    }, '1/R_eq = 1/R₁ + 1/R₂ + 1/R₃'),
    makeEntry({
        en: 'magnetic field of a long straight wire',
        vi: 'từ trường của dây thẳng dài',
        zh: '长直导线的磁场',
        ru: 'магнитное поле прямого проводника',
        ko: '긴 직선 도선의 자기장',
        id: 'medan magnet kawat lurus panjang'
    }, 'B = μ₀ × I / (2π × r)'),
    makeEntry({
        en: 'energy of a simple harmonic oscillator',
        vi: 'năng lượng dao động điều hòa',
        zh: '简谐振子的能量',
        ru: 'энергия гармонического осциллятора',
        ko: '단순 조화 진동자의 에너지',
        id: 'energi osilator harmonik sederhana'
    }, 'E = 1/2 × k × A^2'),
    makeEntry({
        en: 'angular frequency of a spring-mass system',
        vi: 'tần số góc của hệ lò xo - khối',
        zh: '弹簧振子的角频率',
        ru: 'угловая частота системы пружина-масса',
        ko: '스프링-질량계의 각주파수',
        id: 'frekuensi sudut sistem pegas-massa'
    }, 'ω = √(k / m)'),
    makeEntry({
        en: 'period of a spring-mass oscillator',
        vi: 'chu kỳ của dao động lò xo - khối',
        zh: '弹簧振子的周期',
        ru: 'период колебаний пружинного маятника',
        ko: '스프링-질량 진자의 주기',
        id: 'periode osilator pegas-massa'
    }, 'T = 2π × √(m / k)'),
    makeEntry({
        en: 'terminal velocity in a fluid',
        vi: 'vận tốc giới hạn trong chất lỏng',
        zh: '流体中的极限速度',
        ru: 'предельная скорость в жидкости',
        ko: '유체에서의 종단 속도',
        id: 'kecepatan terminal dalam fluida'
    }, 'v_t = √((2 × m × g) / (ρ × A × C_d))'),
    makeEntry({
        en: 'quadratic drag force',
        vi: 'lực cản tỷ lệ bình phương tốc độ',
        zh: '与速度平方成正比的阻力',
        ru: 'квадратичная сила сопротивления',
        ko: '속도 제곱에 비례하는 항력',
        id: 'gaya hambat kuadrat'
    }, 'F_d = 1/2 × ρ × v^2 × C_d × A'),
    makeEntry({
        en: 'pressure at a given depth',
        vi: 'áp suất ở độ sâu',
        zh: '某深度处的压强',
        ru: 'давление на глубине',
        ko: '깊이에 따른 압력',
        id: 'tekanan pada kedalaman tertentu'
    }, 'P = P₀ + ρ × g × h'),
    makeEntry({
        en: 'buoyant force magnitude',
        vi: 'độ lớn lực đẩy Archimedes',
        zh: '浮力大小',
        ru: 'величина силы Архимеда',
        ko: '부력의 크기',
        id: 'besar gaya apung'
    }, 'F_b = ρ × g × V'),
    makeEntry({
        en: 'energy stored in an inductor',
        vi: 'năng lượng trong cuộn cảm',
        zh: '电感中的能量',
        ru: 'энергия, запасенная в индуктивности',
        ko: '인덕터에 저장된 에너지',
        id: 'energi dalam induktor'
    }, 'U = 1/2 × L × I^2'),
    makeEntry({
        en: 'force between parallel currents',
        vi: 'lực giữa hai dòng điện song song',
        zh: '平行电流之间的力',
        ru: 'сила между параллельными токами',
        ko: '평행 전류 사이의 힘',
        id: 'gaya antara arus sejajar'
    }, 'F / L = μ₀ × I₁ × I₂ / (2π × d)'),
    makeEntry({
        en: 'beat frequency of two waves',
        vi: 'tần số nhịp đập của hai sóng',
        zh: '拍频',
        ru: 'частота биений двух волн',
        ko: '두 파동의 맥놀이 주파수',
        id: 'frekuensi beat dua gelombang'
    }, 'f_beats = |f₁ - f₂|'),
    makeEntry({
        en: 'inverse square law for illumination',
        vi: 'định luật nghịch đảo bình phương cho độ rọi',
        zh: '照度的平方反比定律',
        ru: 'закон обратных квадратов для освещенности',
        ko: '조도의 역제곱 법칙',
        id: 'hukum kuadrat terbalik untuk iluminasi'
    }, 'E = I / r^2'),
    makeEntry({
        en: 'buoyancy-based apparent weight',
        vi: 'trọng lượng biểu kiến do lực đẩy',
        zh: '浮力导致的视重',
        ru: 'кажущийся вес при наличии выталкивающей силы',
        ko: '부력에 의한 겉보기 무게',
        id: 'berat semu karena gaya apung'
    }, 'W_apparent = W - F_b'),
    makeEntry({
        en: 'magnetic energy density',
        vi: 'mật độ năng lượng từ trường',
        zh: '磁场能量密度',
        ru: 'плотность энергии магнитного поля',
        ko: '자기장 에너지 밀도',
        id: 'densitas energi magnet'
    }, 'u_B = B^2 / (2μ₀)'),
    makeEntry({
        en: 'electric energy density',
        vi: 'mật độ năng lượng điện trường',
        zh: '电场能量密度',
        ru: 'плотность энергии электрического поля',
        ko: '전기장 에너지 밀도',
        id: 'densitas energi listrik'
    }, 'u_E = 1/2 × ε₀ × E^2'),
    makeEntry({
        en: 'Poynting vector magnitude',
        vi: 'độ lớn véc tơ Poynting',
        zh: '坡印廷矢量的大小',
        ru: 'величина вектора Пойнтинга',
        ko: '포인팅 벡터의 크기',
        id: 'besar vektor Poynting'
    }, 'S = E × H'),
    makeEntry({
        en: 'momentum of a photon',
        vi: 'động lượng của photon',
        zh: '光子的动量',
        ru: 'импульс фотона',
        ko: '광자의 운동량',
        id: 'momentum foton'
    }, 'p = h / λ'),
    makeEntry({
        en: 'Compton wavelength shift',
        vi: 'độ lệch bước sóng Compton',
        zh: '康普顿波长位移',
        ru: 'смещение длины волны Комптона',
        ko: '콤프턴 파장 이동',
        id: 'pergeseran panjang gelombang Compton'
    }, 'Δλ = (h / (m_e × c)) × (1 - cosθ)'),
    makeEntry({
        en: 'relativistic momentum',
        vi: 'động lượng tương đối tính',
        zh: '相对论动量',
        ru: 'релятивистский импульс',
        ko: '상대론적 운동량',
        id: 'momentum relativistik'
    }, 'p = γ × m × v'),
    makeEntry({
        en: 'Planck distribution peak frequency',
        vi: 'tần số cực đại của phân bố Planck',
        zh: '普朗克分布峰值频率',
        ru: 'частота максимума распределения Планка',
        ko: '플랑크 분포의 최대 주파수',
        id: 'frekuensi puncak distribusi Planck'
    }, 'ν_max ≈ 2.82 × k_B × T / h'),
    makeEntry({
        en: 'Bohr quantization of angular momentum',
        vi: 'lượng tử hóa mô men động lượng Bohr',
        zh: '玻尔角动量量子化',
        ru: 'квантование углового момента по Бору',
        ko: '보어의 각운동량 양자화',
        id: 'kuantisasi momentum sudut Bohr'
    }, 'm × v × r = n × ħ'),
    makeEntry({
        en: 'Rydberg formula for hydrogen spectrum',
        vi: 'công thức Rydberg cho quang phổ hydro',
        zh: '氢光谱的里德伯公式',
        ru: 'формула Ридберга для спектра водорода',
        ko: '수소 스펙트럼의 라이드버그 식',
        id: 'rumus Rydberg untuk spektrum hidrogen'
    }, '1/λ = R_H × (1/n₁^2 - 1/n₂^2)'),
    makeEntry({
        en: 'Bragg diffraction condition',
        vi: 'điều kiện nhiễu xạ Bragg',
        zh: '布拉格衍射条件',
        ru: 'условие дифракции Брегга',
        ko: '브래그 회절 조건',
        id: 'kondisi difraksi Bragg'
    }, 'n × λ = 2 × d × sinθ'),
    makeEntry({
        en: 'Schrödinger probability current',
        vi: 'mật độ dòng xác suất Schrödinger',
        zh: '薛定谔概率流密度',
        ru: 'плотность вероятностного тока Шрёдингера',
        ko: '슈뢰딩거 확률 전류',
        id: 'arus probabilitas Schrödinger'
    }, 'j = (ħ / (2mi)) × (ψ*∇ψ - ψ∇ψ*)'),
    makeEntry({
        en: 'uncertainty principle (position-momentum)',
        vi: 'nguyên lý bất định vị trí - động lượng',
        zh: '位置-动量测不准原理',
        ru: 'принцип неопределенности координата-импульс',
        ko: '위치-운동량 불확정성 원리',
        id: 'prinsip ketidakpastian posisi-momentum'
    }, 'Δx × Δp ≥ ħ / 2'),
    makeEntry({
        en: 'uncertainty principle (energy-time)',
        vi: 'nguyên lý bất định năng lượng - thời gian',
        zh: '能量-时间测不准原理',
        ru: 'принцип неопределенности энергия-время',
        ko: '에너지-시간 불확정성 원리',
        id: 'prinsip ketidakpastian energi-waktu'
    }, 'ΔE × Δt ≥ ħ / 2'),
    makeEntry({
        en: 'relativistic energy-momentum relation',
        vi: 'quan hệ năng lượng - động lượng tương đối tính',
        zh: '相对论能量-动量关系',
        ru: 'релятивистская связь энергии и импульса',
        ko: '상대론적 에너지-운동량 관계',
        id: 'hubungan energi-momentum relativistik'
    }, 'E^2 = (pc)^2 + (m₀c^2)^2'),
    makeEntry({
        en: 'magnetic dipole torque',
        vi: 'mô men của moment lưỡng cực từ',
        zh: '磁偶极矩受到的力矩',
        ru: 'крутящий момент магнитного диполя',
        ko: '자기 쌍극자에 작용하는 토크',
        id: 'torsi dipol magnet'
    }, 'τ = μ × B × sinθ'),
    makeEntry({
        en: 'Rayleigh scattering intensity',
        vi: 'cường độ tán xạ Rayleigh',
        zh: '瑞利散射强度',
        ru: 'интенсивность рассеяния Рэлея',
        ko: '레이리 산란 세기',
        id: 'intensitas hamburan Rayleigh'
    }, 'I ∝ 1/λ^4'),
    makeEntry({
        en: 'mass flow rate',
        vi: 'lưu lượng khối',
        zh: '质量流量',
        ru: 'массовый расход',
        ko: '질량 유량',
        id: 'laju aliran massa'
    }, 'ṁ = ρ × A × v'),
    makeEntry({
        en: 'continuity of current',
        vi: 'định luật Kirchhoff dòng điện',
        zh: '基尔霍夫电流定律',
        ru: 'закон Кирхгофа для токов',
        ko: '키르히호프 전류 법칙',
        id: 'hukum arus Kirchhoff'
    }, '∑I_in = ∑I_out'),
    makeEntry({
        en: 'Kirchhoff voltage law',
        vi: 'định luật Kirchhoff điện áp',
        zh: '基尔霍夫电压定律',
        ru: 'закон Кирхгофа для напряжений',
        ko: '키르히호프 전압 법칙',
        id: 'hukum tegangan Kirchhoff'
    }, '∑ΔV_loop = 0'),
    makeEntry({
        en: 'moment of inertia for a solid sphere',
        vi: 'momen quán tính của khối cầu đặc',
        zh: '实心球的转动惯量',
        ru: 'момент инерции сплошного шара',
        ko: '실구의 관성모멘트',
        id: 'momen inersia bola pejal'
    }, 'I = 2/5 × m × r^2'),
    makeEntry({
        en: 'moment of inertia for a solid cylinder',
        vi: 'momen quán tính của trụ đặc',
        zh: '实心圆柱的转动惯量',
        ru: 'момент инерции сплошного цилиндра',
        ko: '실린더의 관성모멘트',
        id: 'momen inersia silinder pejal'
    }, 'I = 1/2 × m × r^2'),
    makeEntry({
        en: 'moment of inertia for a thin rod about center',
        vi: 'momen quán tính của thanh mảnh qua trung điểm',
        zh: '细杆绕中心的转动惯量',
        ru: 'момент инерции тонкого стержня относительно центра',
        ko: '가느다란 막대의 중심 관성모멘트',
        id: 'momen inersia batang tipis melalui pusat'
    }, 'I = 1/12 × m × L^2'),
    makeEntry({
        en: 'moment of inertia for a thin rod about end',
        vi: 'momen quán tính của thanh mảnh quanh đầu mút',
        zh: '细杆绕端点的转动惯量',
        ru: 'момент инерции тонкого стержня относительно конца',
        ko: '막대 끝 기준 관성모멘트',
        id: 'momen inersia batang tipis melalui ujung'
    }, 'I = 1/3 × m × L^2'),
    makeEntry({
        en: 'period of physical pendulum',
        vi: 'chu kỳ con lắc vật lý',
        zh: '物理摆周期',
        ru: 'период физического маятника',
        ko: '물리 진자의 주기',
        id: 'periode bandul fisik'
    }, 'T = 2π × √(I / (m × g × d))'),
    makeEntry({
        en: 'angular frequency of LC circuit',
        vi: 'tần số góc mạch LC',
        zh: 'LC电路的角频率',
        ru: 'угловая частота колебательного контура',
        ko: 'LC 회로의 각주파수',
        id: 'frekuensi sudut rangkaian LC'
    }, 'ω = 1 / √(L × C)'),
    makeEntry({
        en: 'quality factor of resonant circuit',
        vi: 'hệ số chất lượng của mạch cộng hưởng',
        zh: '谐振电路的品质因数',
        ru: 'добротность резонансного контура',
        ko: '공진 회로의 품질 계수',
        id: 'faktor kualitas rangkaian resonansi'
    }, 'Q = ω₀ × L / R'),
    makeEntry({
        en: 'energy density of electromagnetic wave',
        vi: 'mật độ năng lượng sóng điện từ',
        zh: '电磁波能量密度',
        ru: 'плотность энергии электромагнитной волны',
        ko: '전자기파 에너지 밀도',
        id: 'densitas energi gelombang elektromagnetik'
    }, 'u = ε₀ × E^2'),
    makeEntry({
        en: 'angular momentum conservation',
        vi: 'bảo toàn mô men động lượng',
        zh: '角动量守恒',
        ru: 'сохранение углового момента',
        ko: '각운동량 보존',
        id: 'kekekalan momentum sudut'
    }, 'L_initial = L_final'),
    makeEntry({
        en: 'linear momentum conservation',
        vi: 'bảo toàn động lượng tuyến tính',
        zh: '动量守恒',
        ru: 'закон сохранения импульса',
        ko: '선운동량 보존',
        id: 'kekekalan momentum linear'
    }, 'p_initial = p_final'),
    makeEntry({
        en: 'mechanical energy conservation in conservative system',
        vi: 'bảo toàn cơ năng trong hệ bảo toàn',
        zh: '保守系统的机械能守恒',
        ru: 'сохранение механической энергии в консервативной системе',
        ko: '보존계에서의 기계적 에너지 보존',
        id: 'kekekalan energi mekanik dalam sistem konservatif'
    }, 'K + U = const'),
    makeEntry({
        en: 'charge continuity equation',
        vi: 'phương trình liên tục của điện tích',
        zh: '电荷连续性方程',
        ru: 'уравнение непрерывности заряда',
        ko: '전하 연속 방정식',
        id: 'persamaan kontinuitas muatan'
    }, '∂ρ/∂t + ∇·J = 0'),
    makeEntry({
        en: 'Maxwell-Ampère law',
        vi: 'định luật Maxwell-Ampère',
        zh: '麦克斯韦-安培定律',
        ru: 'закон Максвелла — Ампера',
        ko: '맥스웰-암페르 법칙',
        id: 'hukum Maxwell-Ampère'
    }, '∇ × B = μ₀ × J + μ₀ε₀ × ∂E/∂t')
];

const chemistryConcepts = [
    makeEntry({
        en: 'ideal gas law relationship',
        vi: 'mối quan hệ khí lý tưởng',
        zh: '理想气体关系式',
        ru: 'соотношение для идеального газа',
        ko: '이상 기체 관계식',
        id: 'hubungan gas ideal'
    }, 'P × V = n × R × T'),
    makeEntry({
        en: 'combined gas law',
        vi: 'định luật khí kết hợp',
        zh: '综合气体定律',
        ru: 'комбинированный газовый закон',
        ko: '결합 기체 법칙',
        id: 'hukum gas gabungan'
    }, 'P₁ × V₁ / T₁ = P₂ × V₂ / T₂'),
    makeEntry({
        en: "Avogadro's law",
        vi: 'định luật Avogadro',
        zh: '阿伏伽德罗定律',
        ru: 'закон Авогадро',
        ko: '아보가드로 법칙',
        id: 'hukum Avogadro'
    }, 'V / n = k'),
    makeEntry({
        en: "Dalton's law of partial pressures",
        vi: 'định luật Dalton về áp suất riêng phần',
        zh: '道尔顿分压定律',
        ru: 'закон Дальтона о парциальных давлениях',
        ko: '돌턴의 분압 법칙',
        id: 'hukum tekanan parsial Dalton'
    }, 'P_total = ΣP_i'),
    makeEntry({
        en: "Graham's law of effusion",
        vi: 'định luật khuếch tán Graham',
        zh: '格雷厄姆逸出定律',
        ru: 'закон Грэма об эффузии',
        ko: '그레이엄의 확산 법칙',
        id: 'hukum efusi Graham'
    }, 'rate₁ / rate₂ = √(M₂ / M₁)'),
    makeEntry({
        en: 'gas density from molar mass',
        vi: 'khối lượng riêng khí từ khối lượng mol',
        zh: '用摩尔质量求气体密度',
        ru: 'плотность газа через молярную массу',
        ko: '몰질량으로 구한 기체 밀도',
        id: 'massa jenis gas dari massa molar'
    }, 'd = (P × M) / (R × T)'),
    makeEntry({
        en: 'Arrhenius equation',
        vi: 'phương trình Arrhenius',
        zh: '阿伦尼乌斯方程',
        ru: 'уравнение Аррениуса',
        ko: '아레니우스 방정식',
        id: 'persamaan Arrhenius'
    }, 'k = A × e^{-E_a / (R × T)}'),
    makeEntry({
        en: 'Arrhenius form for two temperatures',
        vi: 'dạng Arrhenius cho hai nhiệt độ',
        zh: '两温度下的阿伦尼乌斯式',
        ru: 'форма Аррениуса для двух температур',
        ko: '두 온도에 대한 아레니우스 식',
        id: 'bentuk Arrhenius untuk dua suhu'
    }, 'ln(k₂ / k₁) = -E_a / R × (1/T₂ - 1/T₁)'),
    makeEntry({
        en: 'Gibbs free energy change',
        vi: 'độ biến thiên năng lượng tự do Gibbs',
        zh: '吉布斯自由能变化',
        ru: 'изменение свободной энергии Гиббса',
        ko: '깁스 자유 에너지 변화',
        id: 'perubahan energi bebas Gibbs'
    }, 'ΔG = ΔH - T × ΔS'),
    makeEntry({
        en: 'standard Gibbs energy and equilibrium',
        vi: 'năng lượng tự do Gibbs chuẩn và cân bằng',
        zh: '标准吉布斯能与平衡',
        ru: 'стандартная энергия Гиббса и равновесие',
        ko: '표준 깁스 에너지와 평형',
        id: 'energi Gibbs standar dan kesetimbangan'
    }, 'ΔG° = -R × T × lnK'),
    makeEntry({
        en: 'Nernst equation at 25°C',
        vi: 'phương trình Nernst ở 25°C',
        zh: '25℃下的能斯特方程',
        ru: 'уравнение Нернста при 25°C',
        ko: '25°C에서의 넌스트 식',
        id: 'persamaan Nernst pada 25°C'
    }, 'E = E° - (0.0592 / n) × logQ'),
    makeEntry({
        en: 'general Nernst equation',
        vi: 'phương trình Nernst tổng quát',
        zh: '能斯特方程的一般形式',
        ru: 'общее уравнение Нернста',
        ko: '일반적인 넌스트 식',
        id: 'persamaan Nernst umum'
    }, 'E = E° - (R × T) / (n × F) × lnQ'),
    makeEntry({
        en: 'Henderson–Hasselbalch equation',
        vi: 'phương trình Henderson–Hasselbalch',
        zh: '亨德森-哈塞尔巴赫方程',
        ru: 'уравнение Хендерсона — Хассельбалха',
        ko: '헨더슨-하셀바흐 방정식',
        id: 'persamaan Henderson–Hasselbalch'
    }, 'pH = pK_a + log([A^-]/[HA])'),
    makeEntry({
        en: 'base form of Henderson–Hasselbalch',
        vi: 'dạng bazơ của Henderson–Hasselbalch',
        zh: '碱形式的亨德森方程',
        ru: 'основная форма уравнения Хендерсона',
        ko: '헨더슨 방정식의 염기 형태',
        id: 'bentuk basa Henderson–Hasselbalch'
    }, 'pOH = pK_b + log([BH^+]/[B])'),
    makeEntry({
        en: 'pH definition',
        vi: 'định nghĩa pH',
        zh: 'pH 的定义',
        ru: 'определение pH',
        ko: 'pH 정의',
        id: 'definisi pH'
    }, 'pH = -log[H^+]'),
    makeEntry({
        en: 'pOH definition',
        vi: 'định nghĩa pOH',
        zh: 'pOH 的定义',
        ru: 'определение pOH',
        ko: 'pOH 정의',
        id: 'definisi pOH'
    }, 'pOH = -log[OH^-]'),
    makeEntry({
        en: 'relationship between pH and pOH',
        vi: 'mối liên hệ giữa pH và pOH',
        zh: 'pH 与 pOH 的关系',
        ru: 'связь между pH и pOH',
        ko: 'pH와 pOH의 관계',
        id: 'hubungan pH dan pOH'
    }, 'pH + pOH = 14'),
    makeEntry({
        en: 'ion product of water',
        vi: 'tích số ion của nước',
        zh: '水的离子积',
        ru: 'ионное произведение воды',
        ko: '물의 이온곱',
        id: 'hasil kali ion air'
    }, 'K_w = [H^+] × [OH^-]'),
    makeEntry({
        en: 'acid dissociation constant',
        vi: 'hằng số phân ly axit',
        zh: '酸解离常数',
        ru: 'константа диссоциации кислоты',
        ko: '산 해리 상수',
        id: 'konstanta disosiasi asam'
    }, 'K_a = [H₃O^+] × [A^-] / [HA]'),
    makeEntry({
        en: 'base dissociation constant',
        vi: 'hằng số phân ly bazơ',
        zh: '碱解离常数',
        ru: 'константа диссоциации основания',
        ko: '염기 해리 상수',
        id: 'konstanta disosiasi basa'
    }, 'K_b = [BH^+] × [OH^-] / [B]'),
    makeEntry({
        en: 'relation of Ka and Kb',
        vi: 'mối quan hệ giữa K_a và K_b',
        zh: 'K_a 与 K_b 的关系',
        ru: 'связь K_a и K_b',
        ko: 'Ka와 Kb의 관계',
        id: 'hubungan Ka dan Kb'
    }, 'K_a × K_b = K_w'),
    makeEntry({
        en: 'pKa definition',
        vi: 'định nghĩa pK_a',
        zh: 'pK_a 的定义',
        ru: 'определение pK_a',
        ko: 'pKa 정의',
        id: 'definisi pKa'
    }, 'pK_a = -log K_a'),
    makeEntry({
        en: 'pKb definition',
        vi: 'định nghĩa pK_b',
        zh: 'pK_b 的定义',
        ru: 'определение pK_b',
        ko: 'pKb 정의',
        id: 'definisi pKb'
    }, 'pK_b = -log K_b'),
    makeEntry({
        en: 'relationship between pKa and pKb',
        vi: 'quan hệ giữa pK_a và pK_b',
        zh: 'pK_a 与 pK_b 的关系',
        ru: 'связь pK_a и pK_b',
        ko: 'pKa와 pKb의 관계',
        id: 'hubungan pKa dan pKb'
    }, 'pK_a + pK_b = 14'),
    makeEntry({
        en: 'solubility product expression',
        vi: 'biểu thức tích số tan',
        zh: '溶度积表达式',
        ru: 'выражение произведения растворимости',
        ko: '용해도곱 표현식',
        id: 'ekspresi hasil kali kelarutan'
    }, 'K_sp = [M^{m+}]^m × [X^{n-}]^n'),
    makeEntry({
        en: 'Henry’s law for solubility',
        vi: 'định luật Henry về độ tan',
        zh: '亨利定律',
        ru: 'закон Генри растворимости',
        ko: '헨리의 용해도 법칙',
        id: 'hukum Henry tentang kelarutan'
    }, 'C = k_H × P'),
    makeEntry({
        en: 'Raoult’s law',
        vi: 'định luật Raoult',
        zh: '拉乌尔定律',
        ru: 'закон Рауля',
        ko: '라울의 법칙',
        id: 'hukum Raoult'
    }, 'P_solution = X_solvent × P°'),
    makeEntry({
        en: 'freezing point depression',
        vi: 'hạ điểm đông',
        zh: '降低凝固点',
        ru: 'понижение температуры замерзания',
        ko: '빙점 강하',
        id: 'penurunan titik beku'
    }, 'ΔT_f = K_f × m'),
    makeEntry({
        en: 'boiling point elevation',
        vi: 'nâng điểm sôi',
        zh: '升高沸点',
        ru: 'повышение температуры кипения',
        ko: '비등점 상승',
        id: 'kenaikan titik didih'
    }, 'ΔT_b = K_b × m'),
    makeEntry({
        en: 'osmotic pressure equation',
        vi: 'phương trình áp suất thẩm thấu',
        zh: '渗透压方程',
        ru: 'уравнение осмотического давления',
        ko: '삼투압 방정식',
        id: 'persamaan tekanan osmotik'
    }, 'π = M × R × T'),
    makeEntry({
        en: 'van ’t Hoff factor correction',
        vi: 'hệ số van ’t Hoff',
        zh: '范特霍夫因子',
        ru: 'фактор ван ’т Гоффа',
        ko: '반트호프 인자',
        id: 'faktor van ’t Hoff'
    }, 'π = i × M × R × T'),
    makeEntry({
        en: 'Clausius–Clapeyron equation',
        vi: 'phương trình Clausius–Clapeyron',
        zh: '克劳修斯-克拉佩龙方程',
        ru: 'уравнение Клаузиуса — Клапейрона',
        ko: '클라우지우스-클라페이론 식',
        id: 'persamaan Clausius–Clapeyron'
    }, 'ln(P₂ / P₁) = -ΔH_vap / R × (1/T₂ - 1/T₁)'),
    makeEntry({
        en: 'Henry’s constant version',
        vi: 'dạng hằng số Henry',
        zh: '亨利常数形式',
        ru: 'форма постоянной Генри',
        ko: '헨리 상수 형태',
        id: 'bentuk konstanta Henry'
    }, 'k_H = C / P'),
    makeEntry({
        en: 'Beer–Lambert law',
        vi: 'định luật Beer–Lambert',
        zh: '比耳-朗伯定律',
        ru: 'закон Бугера — Ламберта — Бера',
        ko: '비어-람베르트 법칙',
        id: 'hukum Beer–Lambert'
    }, 'A = ε × l × c'),
    makeEntry({
        en: 'reaction rate law example',
        vi: 'ví dụ phương trình tốc độ phản ứng',
        zh: '反应速率定律示例',
        ru: 'пример кинетического уравнения',
        ko: '반응 속도 법칙 예시',
        id: 'contoh hukum laju reaksi'
    }, 'rate = k × [A]^m × [B]^n'),
    makeEntry({
        en: 'first-order integrated rate law',
        vi: 'phương trình tích phân bậc nhất',
        zh: '一级积分速率方程',
        ru: 'интегральное уравнение скорости первого порядка',
        ko: '1차 반응 적분 속도식',
        id: 'hukum laju terintegral orde pertama'
    }, 'ln[A] = -k × t + ln[A]_0'),
    makeEntry({
        en: 'second-order integrated rate law',
        vi: 'phương trình tích phân bậc hai',
        zh: '二级积分速率方程',
        ru: 'интегральное уравнение скорости второго порядка',
        ko: '2차 반응 적분 속도식',
        id: 'hukum laju terintegral orde kedua'
    }, '1/[A] = k × t + 1/[A]_0'),
    makeEntry({
        en: 'zero-order integrated rate law',
        vi: 'phương trình tích phân bậc không',
        zh: '零级积分速率方程',
        ru: 'интегральное уравнение скорости нулевого порядка',
        ko: '0차 반응 적분 속도식',
        id: 'hukum laju terintegral orde nol'
    }, '[A] = -k × t + [A]_0'),
    makeEntry({
        en: 'first-order half-life',
        vi: 'chu kỳ bán rã bậc nhất',
        zh: '一级反应半衰期',
        ru: 'период полураспада реакции первого порядка',
        ko: '1차 반응 반감기',
        id: 'waktu paruh orde pertama'
    }, 't_1/2 = 0.693 / k'),
    makeEntry({
        en: 'Arrhenius activation energy from slope',
        vi: 'năng lượng hoạt hóa từ đồ thị Arrhenius',
        zh: '通过阿伦尼乌斯图求活化能',
        ru: 'активационная энергия по графику Аррениуса',
        ko: '아레니우스 그래프에서의 활성화 에너지',
        id: 'energi aktivasi dari grafik Arrhenius'
    }, 'E_a = -R × slope'),
    makeEntry({
        en: 'reaction quotient definition',
        vi: 'định nghĩa thương số phản ứng',
        zh: '反应商的定义',
        ru: 'определение реакционного частного',
        ko: '반응 지수의 정의',
        id: 'definisi kuosien reaksi'
    }, 'Q = [C]^c × [D]^d / ([A]^a × [B]^b)'),
    makeEntry({
        en: 'equilibrium constant expression',
        vi: 'biểu thức hằng số cân bằng',
        zh: '平衡常数表达式',
        ru: 'выражение константы равновесия',
        ko: '평형 상수 표현식',
        id: 'ekspresi konstanta kesetimbangan'
    }, 'K = [C]^c × [D]^d / ([A]^a × [B]^b)'),
    makeEntry({
        en: 'relation of Kp and Kc',
        vi: 'quan hệ K_p và K_c',
        zh: 'K_p 与 K_c 的关系',
        ru: 'связь K_p и K_c',
        ko: 'Kp와 Kc의 관계',
        id: 'hubungan Kp dan Kc'
    }, 'K_p = K_c × (R × T)^{Δn}'),
    makeEntry({
        en: 'Gibbs free energy at nonstandard conditions',
        vi: 'năng lượng tự do Gibbs ngoài chuẩn',
        zh: '非标准条件下的吉布斯自由能',
        ru: 'свободная энергия Гиббса при нестандартных условиях',
        ko: '표준 외 조건의 깁스 자유에너지',
        id: 'energi Gibbs pada kondisi non-standar'
    }, 'ΔG = ΔG° + R × T × lnQ'),
    makeEntry({
        en: 'electrochemical cell free energy',
        vi: 'năng lượng tự do của pin điện hóa',
        zh: '电化学电池的自由能',
        ru: 'свободная энергия гальванического элемента',
        ko: '전기화학 전지의 자유 에너지',
        id: 'energi bebas sel elektrokimia'
    }, 'ΔG = -n × F × E'),
    makeEntry({
        en: 'Faraday’s electrolysis law',
        vi: 'định luật điện phân Faraday',
        zh: '法拉第电解定律',
        ru: 'закон электролиза Фарадея',
        ko: '패러데이 전기분해 법칙',
        id: 'hukum elektrolisis Faraday'
    }, 'm = (Q × M) / (n × F)'),
    makeEntry({
        en: 'moles from mass',
        vi: 'tính mol từ khối lượng',
        zh: '由质量求摩尔数',
        ru: 'нахождение молей по массе',
        ko: '질량으로 몰수 구하기',
        id: 'mol dari massa'
    }, 'n = m / M'),
    makeEntry({
        en: 'molarity definition',
        vi: 'định nghĩa nồng độ mol',
        zh: '摩尔浓度定义',
        ru: 'определение молярности',
        ko: '몰 농도의 정의',
        id: 'definisi molaritas'
    }, 'M = n / V'),
    makeEntry({
        en: 'molality definition',
        vi: 'định nghĩa độ molan',
        zh: '摩尔质量浓度定义',
        ru: 'определение моляльности',
        ko: '몰랄 농도의 정의',
        id: 'definisi molalitas'
    }, 'm = n_solute / kg_solvent'),
    makeEntry({
        en: 'normality definition',
        vi: 'định nghĩa đương lượng',
        zh: '当量浓度定义',
        ru: 'определение нормальности',
        ko: '노르말 농도의 정의',
        id: 'definisi normalitas'
    }, 'N = equivalents / L'),
    makeEntry({
        en: 'dilution equation',
        vi: 'phương trình pha loãng',
        zh: '稀释公式',
        ru: 'уравнение разбавления',
        ko: '희석 방정식',
        id: 'persamaan pengenceran'
    }, 'M₁ × V₁ = M₂ × V₂'),
    makeEntry({
        en: 'mole fraction definition',
        vi: 'định nghĩa phần mol',
        zh: '摩尔分数定义',
        ru: 'определение мольной доли',
        ko: '몰분율 정의',
        id: 'definisi fraksi mol'
    }, 'X_i = n_i / n_total'),
    makeEntry({
        en: 'percent composition by mass',
        vi: 'phần trăm khối lượng',
        zh: '质量百分含量',
        ru: 'массовая доля компонента',
        ko: '질량 백분율',
        id: 'persen komposisi massa'
    }, 'mass% = (mass_component / mass_total) × 100%'),
    makeEntry({
        en: 'theoretical yield calculation',
        vi: 'tính hiệu suất lý thuyết',
        zh: '理论产量计算',
        ru: 'расчет теоретического выхода',
        ko: '이론 수율 계산',
        id: 'perhitungan hasil teoritis'
    }, 'n_product = limiting moles × ratio'),
    makeEntry({
        en: 'percent yield formula',
        vi: 'công thức hiệu suất phần trăm',
        zh: '百分产率公式',
        ru: 'формула процентного выхода',
        ko: '수율 백분율 공식',
        id: 'rumus persen hasil'
    }, 'percent yield = (actual / theoretical) × 100%'),
    makeEntry({
        en: 'enthalpy change from bond energies',
        vi: 'độ biến thiên entanpi từ năng lượng liên kết',
        zh: '用键能计算焓变',
        ru: 'изменение энтальпии по энергиям связей',
        ko: '결합 에너지로 계산한 엔탈피 변화',
        id: 'perubahan entalpi dari energi ikatan'
    }, 'ΔH = ΣE_broken - ΣE_formed'),
    makeEntry({
        en: 'Hess’s law summation',
        vi: 'tổng hợp theo định luật Hess',
        zh: '亥斯定律求和',
        ru: 'суммирование по закону Гесса',
        ko: '헤스 법칙 합산',
        id: 'penjumlahan menurut hukum Hess'
    }, 'ΔH_total = ΣΔH_steps'),
    makeEntry({
        en: 'entropy change for phase transition',
        vi: 'biến thiên entropy khi chuyển pha',
        zh: '相变的熵变',
        ru: 'изменение энтропии при фазовом переходе',
        ko: '상전이의 엔트로피 변화',
        id: 'perubahan entropi saat transisi fasa'
    }, 'ΔS = ΔH_transition / T'),
    makeEntry({
        en: 'heat released or absorbed',
        vi: 'nhiệt lượng tỏa ra hoặc hấp thụ',
        zh: '放出或吸收的热量',
        ru: 'выделенное или поглощенное тепло',
        ko: '방출되거나 흡수된 열량',
        id: 'kalor yang dilepas atau diserap'
    }, 'q = m × c × ΔT'),
    makeEntry({
        en: 'calorimetry balance',
        vi: 'cân bằng nhiệt lượng kế',
        zh: '量热平衡',
        ru: 'баланс в калориметре',
        ko: '열량계 평형',
        id: 'keseimbangan kalorimetri'
    }, 'q_lost + q_gained = 0'),
    makeEntry({
        en: 'gas effusion time relation',
        vi: 'quan hệ thời gian thoát khí',
        zh: '气体逸出时间关系',
        ru: 'соотношение времени эффузии газов',
        ko: '기체 방출 시간 관계',
        id: 'hubungan waktu efusi gas'
    }, 't₁ / t₂ = √(M₁ / M₂)'),
    makeEntry({
        en: 'electroneutrality condition',
        vi: 'điều kiện trung hòa điện',
        zh: '电中性条件',
        ru: 'условие электронейтральности',
        ko: '전기적 중성 조건',
        id: 'kondisi kenetralan listrik'
    }, 'Σ positive charge = Σ negative charge'),
    makeEntry({
        en: 'mass percent concentration',
        vi: 'nồng độ phần trăm khối lượng',
        zh: '质量百分浓度',
        ru: 'массовая процентная концентрация',
        ko: '질량 백분율 농도',
        id: 'konsentrasi persen massa'
    }, 'w/w% = (mass_solute / mass_solution) × 100%'),
    makeEntry({
        en: 'volume percent concentration',
        vi: 'nồng độ phần trăm thể tích',
        zh: '体积分数浓度',
        ru: 'объемная процентная концентрация',
        ko: '부피 백분율 농도',
        id: 'konsentrasi persen volume'
    }, 'v/v% = (volume_solute / volume_solution) × 100%'),
    makeEntry({
        en: 'parts per million calculation',
        vi: 'tính phần triệu',
        zh: '百万分率计算',
        ru: 'расчет ppm',
        ko: '백만분율 계산',
        id: 'perhitungan ppm'
    }, 'ppm = (mass_solute / mass_solution) × 10^6'),
    makeEntry({
        en: 'net ionic equation for neutralization',
        vi: 'phương trình ion ròng của phản ứng trung hòa',
        zh: '中和反应的净离子方程',
        ru: 'чистое ионное уравнение нейтрализации',
        ko: '중화 반응의 순이온식',
        id: 'persamaan ion bersih penetralan'
    }, 'H^+ + OH^- → H₂O'),
    makeEntry({
        en: 'water electrolysis reaction',
        vi: 'phản ứng điện phân nước',
        zh: '水的电解反应',
        ru: 'реакция электролиза воды',
        ko: '물의 전기분해 반응',
        id: 'reaksi elektrolisis air'
    }, '2H₂O → 2H₂ + O₂'),
    makeEntry({
        en: 'Haber process synthesis',
        vi: 'tổng hợp theo quy trình Haber',
        zh: '哈柏合成法',
        ru: 'процесс Габера',
        ko: '하버 공정 합성',
        id: 'sintesis proses Haber'
    }, 'N₂ + 3H₂ ⇌ 2NH₃'),
    makeEntry({
        en: 'contact process oxidation',
        vi: 'oxi hóa trong quy trình tiếp xúc',
        zh: '接触法氧化',
        ru: 'окисление в контактном процессе',
        ko: '콘택트 공정 산화',
        id: 'oksidasi proses kontak'
    }, '2SO₂ + O₂ ⇌ 2SO₃'),
    makeEntry({
        en: 'combustion of methane',
        vi: 'phản ứng cháy của methane',
        zh: '甲烷燃烧反应',
        ru: 'сгорание метана',
        ko: '메탄 연소 반응',
        id: 'pembakaran metana'
    }, 'CH₄ + 2O₂ → CO₂ + 2H₂O'),
    makeEntry({
        en: 'photosynthesis overall reaction',
        vi: 'phản ứng tổng quát quang hợp',
        zh: '光合作用总反应',
        ru: 'общая реакция фотосинтеза',
        ko: '광합성 총 반응',
        id: 'reaksi keseluruhan fotosintesis'
    }, '6CO₂ + 6H₂O → C₆H₁₂O₆ + 6O₂'),
    makeEntry({
        en: 'respiration summary reaction',
        vi: 'phản ứng tổng quát hô hấp',
        zh: '呼吸总反应',
        ru: 'суммарная реакция дыхания',
        ko: '호흡의 총 반응',
        id: 'reaksi ringkas respirasi'
    }, 'C₆H₁₂O₆ + 6O₂ → 6CO₂ + 6H₂O'),
    makeEntry({
        en: 'neutralization of hydrochloric acid with sodium hydroxide',
        vi: 'trung hòa HCl bằng NaOH',
        zh: '盐酸与氢氧化钠中和',
        ru: 'нейтрализация HCl NaOH',
        ko: '염산과 수산화나트륨의 중화',
        id: 'netralisasi HCl dengan NaOH'
    }, 'HCl + NaOH → NaCl + H₂O'),
    makeEntry({
        en: 'decomposition of hydrogen peroxide',
        vi: 'phân hủy hydro peroxit',
        zh: '过氧化氢分解',
        ru: 'разложение перекиси водорода',
        ko: '과산화수소 분해',
        id: 'dekomposisi hidrogen peroksida'
    }, '2H₂O₂ → 2H₂O + O₂'),
    makeEntry({
        en: 'formation of rust',
        vi: 'sự hình thành gỉ sắt',
        zh: '铁锈的形成',
        ru: 'образование ржавчины',
        ko: '녹 형성',
        id: 'pembentukan karat'
    }, '4Fe + 3O₂ → 2Fe₂O₃'),
    makeEntry({
        en: 'decomposition of calcium carbonate',
        vi: 'phân hủy canxi cacbonat',
        zh: '碳酸钙分解',
        ru: 'разложение карбоната кальция',
        ko: '탄산칼슘 분해',
        id: 'dekomposisi kalsium karbonat'
    }, 'CaCO₃ → CaO + CO₂'),
    makeEntry({
        en: 'formation of ammonia from ammonium chloride and sodium hydroxide',
        vi: 'tạo amoniac từ NH₄Cl và NaOH',
        zh: '氯化铵与氢氧化钠生成氨',
        ru: 'получение аммиака из NH₄Cl и NaOH',
        ko: '염화암모늄과 가성소다로 암모니아 생성',
        id: 'pembentukan amonia dari NH₄Cl dan NaOH'
    }, 'NH₄Cl + NaOH → NH₃ + NaCl + H₂O'),
    makeEntry({
        en: 'formation of carbonic acid',
        vi: 'hình thành axit carbonic',
        zh: '碳酸的形成',
        ru: 'образование угольной кислоты',
        ko: '탄산 생성',
        id: 'pembentukan asam karbonat'
    }, 'CO₂ + H₂O ⇌ H₂CO₃'),
    makeEntry({
        en: 'esterification of acetic acid with ethanol',
        vi: 'este hóa axit axetic với etanol',
        zh: '乙酸与乙醇酯化',
        ru: 'этерификация уксусной кислоты этанолом',
        ko: '아세트산과 에탄올의 에스터화',
        id: 'esterifikasi asam asetat dengan etanol'
    }, 'CH₃COOH + C₂H₅OH ⇌ CH₃COOC₂H₅ + H₂O'),
    makeEntry({
        en: 'saponification of triglyceride',
        vi: 'xà phòng hóa triglyceride',
        zh: '甘油三酯皂化',
        ru: 'омыление триглицерида',
        ko: '트라이글리세리드 비누화',
        id: 'saponifikasi trigliserida'
    }, 'fat + 3NaOH → glycerol + 3soap'),
    makeEntry({
        en: 'Hofmann elimination example',
        vi: 'ví dụ phản ứng loại Hofmann',
        zh: '霍夫曼消除实例',
        ru: 'пример элиминирования Хоффмана',
        ko: '호프만 제거 반응 예',
        id: 'contoh eliminasi Hofmann'
    }, 'R₄N^+OH^- → alkene + tertiary amine + H₂O'),
    makeEntry({
        en: 'Markovnikov addition of HBr to propene',
        vi: 'cộng HBr theo Markovnikov vào propene',
        zh: 'HBr 马氏加成到丙烯',
        ru: 'присоединение HBr к пропену по правилу Марковникова',
        ko: 'HBr의 마르코프니코프 부가',
        id: 'adisi Markovnikov HBr pada propena'
    }, 'CH₃-CH=CH₂ + HBr → CH₃-CHBr-CH₃'),
    makeEntry({
        en: 'anti-Markovnikov addition using peroxide',
        vi: 'cộng HBr theo anti-Markovnikov với peroxide',
        zh: '过氧化物条件下的反马氏加成',
        ru: 'анти-Марковниковское присоединение с пероксидами',
        ko: '과산화물 조건의 안티 마르코프니코프 부가',
        id: 'adisi anti-Markovnikov dengan peroksida'
    }, 'CH₃-CH=CH₂ + HBr (ROOR) → CH₃-CH₂-CH₂Br'),
    makeEntry({
        en: 'hydration of ethene to ethanol',
        vi: 'hydrat hóa etilen thành etanol',
        zh: '乙烯水合生成乙醇',
        ru: 'гидратация этена до этанола',
        ko: '에틸렌의 수화로 에탄올 생성',
        id: 'hidrasi etena menjadi etanol'
    }, 'CH₂=CH₂ + H₂O → CH₃-CH₂OH'),
    makeEntry({
        en: 'oxidation of ethanol to acetic acid',
        vi: 'oxi hóa etanol thành axit axetic',
        zh: '乙醇氧化为乙酸',
        ru: 'окисление этанола до уксусной кислоты',
        ko: '에탄올을 아세트산으로 산화',
        id: 'oksidasi etanol menjadi asam asetat'
    }, 'CH₃-CH₂OH + O₂ → CH₃COOH + H₂O'),
    makeEntry({
        en: 'formation of ozone from oxygen',
        vi: 'hình thành ozone từ oxy',
        zh: '氧气生成臭氧',
        ru: 'образование озона из кислорода',
        ko: '산소에서 오존 형성',
        id: 'pembentukan ozon dari oksigen'
    }, '3O₂ → 2O₃'),
    makeEntry({
        en: 'ozone decomposition back to oxygen',
        vi: 'phân hủy ozone thành oxy',
        zh: '臭氧分解为氧',
        ru: 'разложение озона на кислород',
        ko: '오존의 산소로 분해',
        id: 'dekomposisi ozon menjadi oksigen'
    }, '2O₃ → 3O₂'),
    makeEntry({
        en: 'formation of nitric acid from ammonia (Ostwald process)',
        vi: 'tạo axit nitric từ amoniac (quy trình Ostwald)',
        zh: '奥斯特瓦尔德法制硝酸',
        ru: 'получение азотной кислоты по Оствальду',
        ko: '오스트발트 공정의 질산 생산',
        id: 'pembentukan asam nitrat proses Ostwald'
    }, '4NH₃ + 5O₂ → 4NO + 6H₂O'),
    makeEntry({
        en: 'oxidation of NO to NO₂',
        vi: 'oxi hóa NO thành NO₂',
        zh: '一氧化氮氧化为二氧化氮',
        ru: 'окисление NO до NO₂',
        ko: 'NO를 NO₂로 산화',
        id: 'oksidasi NO menjadi NO₂'
    }, '2NO + O₂ → 2NO₂'),
    makeEntry({
        en: 'absorption of NO₂ in water to form nitric acid',
        vi: 'hấp thụ NO₂ vào nước tạo axit nitric',
        zh: '二氧化氮与水生成硝酸',
        ru: 'поглощение NO₂ водой с образованием HNO₃',
        ko: 'NO₂가 물에 흡수되어 질산 형성',
        id: 'absorpsi NO₂ dalam air membentuk asam nitrat'
    }, '3NO₂ + H₂O → 2HNO₃ + NO'),
    makeEntry({
        en: 'silver mirror test reaction',
        vi: 'phản ứng tráng bạc',
        zh: '银镜反应',
        ru: 'реакция серебряного зеркала',
        ko: '은거울 반응',
        id: 'reaksi cermin perak'
    }, 'R-CHO + 2Ag^+ + H₂O → R-COOH + 2Ag + 2H^+'),
    makeEntry({
        en: 'combustion of ethanol',
        vi: 'phản ứng cháy của etanol',
        zh: '乙醇燃烧',
        ru: 'сгорание этанола',
        ko: '에탄올 연소',
        id: 'pembakaran etanol'
    }, 'C₂H₅OH + 3O₂ → 2CO₂ + 3H₂O'),
    makeEntry({
        en: 'combustion of benzene',
        vi: 'phản ứng cháy của benzen',
        zh: '苯燃烧',
        ru: 'сгорание бензола',
        ko: '벤젠 연소',
        id: 'pembakaran benzena'
    }, '2C₆H₆ + 15O₂ → 12CO₂ + 6H₂O'),
    makeEntry({
        en: 'formation of PVC from vinyl chloride polymerization',
        vi: 'trùng hợp vinyl clorua tạo PVC',
        zh: '氯乙烯聚合生成聚氯乙烯',
        ru: 'полимеризация винилхлорида в ПВХ',
        ko: '염화비닐 중합으로 PVC 생성',
        id: 'pembentukan PVC dari polimerisasi vinil klorida'
    }, 'n(CH₂=CHCl) → –(CH₂–CHCl)–_n'),
    makeEntry({
        en: 'dehydration of 2-propanol to propene',
        vi: 'khử nước 2-propanol thành propene',
        zh: '2-丙醇脱水生成丙烯',
        ru: 'обезвоживание 2-пропанола до пропена',
        ko: '2-프로판올 탈수로 프로펜 생성',
        id: 'dehidrasi 2-propanol menjadi propena'
    }, '(CH₃)₂CHOH → CH₃-CH=CH₂ + H₂O'),
    makeEntry({
        en: 'formation of sodium bicarbonate from sodium carbonate and CO₂',
        vi: 'tạo natri bicarbonat từ natri carbonat và CO₂',
        zh: '碳酸钠与二氧化碳生成碳酸氢钠',
        ru: 'образование гидрокарбоната натрия из карбоната и CO₂',
        ko: '탄산나트륨과 이산화탄소로 중조 생성',
        id: 'pembentukan natrium bikarbonat dari natrium karbonat dan CO₂'
    }, 'Na₂CO₃ + CO₂ + H₂O → 2NaHCO₃'),
    makeEntry({
        en: 'decomposition of sodium bicarbonate',
        vi: 'phân hủy natri bicarbonat',
        zh: '碳酸氢钠分解',
        ru: 'разложение гидрокарбоната натрия',
        ko: '중조 분해',
        id: 'dekomposisi natrium bikarbonat'
    }, '2NaHCO₃ → Na₂CO₃ + CO₂ + H₂O'),
    makeEntry({
        en: 'formation of calcium sulfate from calcium carbonate and sulfuric acid',
        vi: 'tạo canxi sulfat từ canxi carbonat và axit sulfuric',
        zh: '碳酸钙与硫酸生成硫酸钙',
        ru: 'образование сульфата кальция из карбоната и серной кислоты',
        ko: '탄산칼슘과 황산으로 황산칼슘 생성',
        id: 'pembentukan kalsium sulfat dari kalsium karbonat dan asam sulfat'
    }, 'CaCO₃ + H₂SO₄ → CaSO₄ + CO₂ + H₂O'),
    makeEntry({
        en: 'chloroform formation from methane chlorination',
        vi: 'tạo chloroform khi clo hóa methane',
        zh: '甲烷氯化生成氯仿',
        ru: 'образование хлороформа при хлорировании метана',
        ko: '메탄 염소화로 클로로포름 생성',
        id: 'pembentukan kloroform dari klorinasi metana'
    }, 'CH₄ + 3Cl₂ → CHCl₃ + 3HCl'),
    makeEntry({
        en: 'polymerization of ethene to polyethylene',
        vi: 'trùng hợp etylen tạo polyethylene',
        zh: '乙烯聚合成聚乙烯',
        ru: 'полимеризация этилена в полиэтилен',
        ko: '에틸렌의 중합으로 폴리에틸렌 생성',
        id: 'polimerisasi etena menjadi polietilena'
    }, 'n(CH₂=CH₂) → –(CH₂–CH₂)–_n'),
    makeEntry({
        en: 'formation of ethyl acetate from acetyl chloride and ethanol',
        vi: 'tạo etyl axetat từ acetyl chloride và etanol',
        zh: '乙酰氯与乙醇生成乙酸乙酯',
        ru: 'образование этилацетата из ацетилхлорида и этанола',
        ko: '아세틸 클로라이드와 에탄올로 에틸아세테이트 생성',
        id: 'pembentukan etil asetat dari asetil klorida dan etanol'
    }, 'CH₃COCl + C₂H₅OH → CH₃COOC₂H₅ + HCl'),
    makeEntry({
        en: 'Wurtz coupling of methyl bromide',
        vi: 'phản ứng ghép Wurtz của methyl bromide',
        zh: '甲基溴的武慈偶联',
        ru: 'сочетание Вюрца метилбромида',
        ko: '메틸 브로마이드의 부르츠 결합',
        id: 'kopling Wurtz metil bromida'
    }, '2CH₃Br + 2Na → C₂H₆ + 2NaBr'),
    makeEntry({
        en: 'grignard formation from bromobenzene and magnesium',
        vi: 'tạo thuốc thử Grignard từ bromobenzen và magiê',
        zh: '溴苯与镁制备格氏试剂',
        ru: 'образование реагента Гриньяра из бромбензола',
        ko: '브로모벤젠과 마그네슘으로 그리냐르 시약 생성',
        id: 'pembentukan Grignard dari bromobenzena dan magnesium'
    }, 'C₆H₅Br + Mg → C₆H₅MgBr'),
    makeEntry({
        en: 'Friedel–Crafts alkylation of benzene with CH₃Cl',
        vi: 'ankyl hóa Friedel–Crafts của benzen bằng CH₃Cl',
        zh: '甲基氯对苯的傅-克烷基化',
        ru: 'алкилирование Фриделя — Крафтса бензола CH₃Cl',
        ko: 'CH₃Cl을 이용한 프리델-크래프츠 알킬화',
        id: 'alkilasi Friedel–Crafts benzena dengan CH₃Cl'
    }, 'C₆H₆ + CH₃Cl → C₆H₅CH₃ + HCl'),
    makeEntry({
        en: 'Diels–Alder reaction example',
        vi: 'ví dụ phản ứng Diels–Alder',
        zh: '狄尔斯-阿尔德反应示例',
        ru: 'пример реакции Дильса — Альдера',
        ko: '딜스-알더 반응 예',
        id: 'contoh reaksi Diels–Alder'
    }, 'butadiene + ethene → cyclohexene'),
    makeEntry({
        en: 'formation of nylon-6,6',
        vi: 'tạo nylon-6,6',
        zh: '尼龙-6,6 的形成',
        ru: 'образование нейлона-6,6',
        ko: '나일론-6,6 합성',
        id: 'pembentukan nilon-6,6'
    }, 'hexamethylenediamine + adipoyl chloride → nylon + 2HCl'),
    makeEntry({
        en: 'combustion of hydrogen gas',
        vi: 'phản ứng cháy của hydro',
        zh: '氢气燃烧',
        ru: 'сгорание водорода',
        ko: '수소 연소',
        id: 'pembakaran hidrogen'
    }, '2H₂ + O₂ → 2H₂O'),
    makeEntry({
        en: 'thermite reaction',
        vi: 'phản ứng nhiệt nhôm',
        zh: '铝热反应',
        ru: 'термитная реакция',
        ko: '테르밋 반응',
        id: 'reaksi termit'
    }, 'Fe₂O₃ + 2Al → 2Fe + Al₂O₃'),
    makeEntry({
        en: 'formation of sodium chloride from elements',
        vi: 'tạo natri clorua từ nguyên tố',
        zh: '由元素生成氯化钠',
        ru: 'образование хлорида натрия из элементов',
        ko: '원소에서 염화나트륨 생성',
        id: 'pembentukan natrium klorida dari unsur'
    }, '2Na + Cl₂ → 2NaCl'),
    makeEntry({
        en: 'formation of sulfuric acid from SO₃',
        vi: 'tạo axit sulfuric từ SO₃',
        zh: '三氧化硫制硫酸',
        ru: 'образование серной кислоты из SO₃',
        ko: 'SO₃로부터 황산 생성',
        id: 'pembentukan asam sulfat dari SO₃'
    }, 'SO₃ + H₂O → H₂SO₄'),
    makeEntry({
        en: 'neutralization of sulfuric acid with potassium hydroxide',
        vi: 'trung hòa H₂SO₄ bằng KOH',
        zh: '硫酸与氢氧化钾中和',
        ru: 'нейтрализация серной кислоты KOH',
        ko: '황산과 수산화칼륨의 중화',
        id: 'netralisasi asam sulfat dengan KOH'
    }, 'H₂SO₄ + 2KOH → K₂SO₄ + 2H₂O'),
    makeEntry({
        en: 'neutralization of nitric acid with calcium hydroxide',
        vi: 'trung hòa HNO₃ bằng Ca(OH)₂',
        zh: '硝酸与氢氧化钙中和',
        ru: 'нейтрализация азотной кислоты гидроксидом кальция',
        ko: '질산과 수산화칼슘의 중화',
        id: 'netralisasi asam nitrat dengan Ca(OH)₂'
    }, '2HNO₃ + Ca(OH)₂ → Ca(NO₃)₂ + 2H₂O'),
    makeEntry({
        en: 'precipitation of silver chloride',
        vi: 'kết tủa bạc clorua',
        zh: '氯化银沉淀',
        ru: 'осаждение хлорида серебра',
        ko: '염화은 침전',
        id: 'pengendapan perak klorida'
    }, 'AgNO₃ + NaCl → AgCl↓ + NaNO₃'),
    makeEntry({
        en: 'precipitation of barium sulfate',
        vi: 'kết tủa bari sulfat',
        zh: '硫酸钡沉淀',
        ru: 'осаждение сульфата бария',
        ko: '황산바륨 침전',
        id: 'pengendapan barium sulfat'
    }, 'BaCl₂ + Na₂SO₄ → BaSO₄↓ + 2NaCl'),
    makeEntry({
        en: 'double displacement producing calcium carbonate',
        vi: 'phản ứng trao đổi kép tạo canxi carbonat',
        zh: '生成碳酸钙的复分解反应',
        ru: 'реакция двойного обмена с образованием CaCO₃',
        ko: '탄산칼슘을 생성하는 복이온 치환',
        id: 'pertukaran ganda menghasilkan kalsium karbonat'
    }, 'CaCl₂ + Na₂CO₃ → CaCO₃↓ + 2NaCl'),
    makeEntry({
        en: 'acid decomposition of bicarbonate',
        vi: 'phản ứng phân hủy bicacbonat bởi axit',
        zh: '碳酸氢盐的酸分解',
        ru: 'разложение гидрокарбоната кислотой',
        ko: '탄산수소염의 산 분해',
        id: 'dekomposisi bikarbonat oleh asam'
    }, 'NaHCO₃ + HCl → NaCl + CO₂ + H₂O'),
    makeEntry({
        en: 'formation of ammonia from ammonium sulfate and calcium hydroxide',
        vi: 'tạo amoniac từ (NH₄)₂SO₄ và Ca(OH)₂',
        zh: '硫酸铵与氢氧化钙生成氨',
        ru: 'получение аммиака из (NH₄)₂SO₄ и Ca(OH)₂',
        ko: '황산암모늄과 수산화칼슘으로 암모니아 생성',
        id: 'pembentukan amonia dari (NH₄)₂SO₄ dan Ca(OH)₂'
    }, '(NH₄)₂SO₄ + Ca(OH)₂ → 2NH₃ + CaSO₄ + 2H₂O'),
    makeEntry({
        en: 'oxidation of ferrous ion to ferric ion',
        vi: 'oxi hóa Fe²⁺ thành Fe³⁺',
        zh: '亚铁离子氧化为铁(III)',
        ru: 'окисление Fe²⁺ до Fe³⁺',
        ko: 'Fe²⁺의 Fe³⁺로 산화',
        id: 'oksidasi ion ferro menjadi fero'
    }, '4Fe²⁺ + O₂ + 4H^+ → 4Fe³⁺ + 2H₂O'),
    makeEntry({
        en: 'reduction of permanganate in acidic solution',
        vi: 'khử permanganat trong môi trường axit',
        zh: '酸性条件下高锰酸根的还原',
        ru: 'восстановление перманганата в кислой среде',
        ko: '산성 용액에서 퍼맨가네이트의 환원',
        id: 'reduksi permanganat dalam larutan asam'
    }, 'MnO₄^- + 8H^+ + 5e^- → Mn²⁺ + 4H₂O'),
    makeEntry({
        en: 'oxidation of iodide by chlorine',
        vi: 'oxi hóa iodua bằng clo',
        zh: '氯气氧化碘离子',
        ru: 'окисление иодида хлором',
        ko: '염소에 의한 아이오딘화물 산화',
        id: 'oksidasi iodida oleh klorin'
    }, 'Cl₂ + 2I^- → 2Cl^- + I₂'),
    makeEntry({
        en: 'disproportionation of chlorine in alkali',
        vi: 'phản ứng tự oxi hóa - khử của clo trong kiềm',
        zh: '氯在碱中歧化',
        ru: 'диспропорционирование хлора в щелочи',
        ko: '염소의 염기성 용액 불균등화',
        id: 'disproporsionasi klorin dalam basa'
    }, 'Cl₂ + 2OH^- → Cl^- + ClO^- + H₂O')
];

const okxQuestions = [
    makeEntry({ en: "Which public chain powers the OKX Web3 ecosystem?" }, 'OKX Chain (OKC)'),
    makeEntry({ en: "What is the native token used to pay fees on OKX Chain?" }, 'OKT'),
    makeEntry({ en: "Which OKX feature lets users swap tokens across multiple chains in one interface?" }, 'OKX DEX aggregator'),
    makeEntry({ en: "What name does OKX use for its self-custodial mobile wallet?" }, 'OKX Web3 Wallet'),
    makeEntry({ en: "Where can users explore decentralized apps within OKX's wallet?" }, 'Discover tab'),
    makeEntry({ en: "Which OKX tool provides insights into market depth and liquidity for pairs?" }, 'Order book'),
    makeEntry({ en: "What OKX product allows copy trading of professional traders?" }, 'Copy Trading'),
    makeEntry({ en: "Which section on OKX highlights limited-time trading competitions?" }, 'Trading competitions center'),
    makeEntry({ en: "What learning hub does OKX provide for tutorials and guides?" }, 'OKX Academy'),
    makeEntry({ en: "Which OKX program rewards users for completing interactive quests?" }, 'OKX Jumpstart Mining'),
    makeEntry({ en: "What is the official domain for the OKX Web3 portal?" }, 'web3.okx.com'),
    makeEntry({ en: "Which OKX feature lets users bridge assets between chains?" }, 'Cross-chain bridge'),
    makeEntry({ en: "What identity standard does OKX Wallet support for human-readable names?" }, 'OKX Name Service (OKXNS)'),
    makeEntry({ en: "Which OKX service offers periodic yield opportunities from exchanges and DeFi?" }, 'Earn'),
    makeEntry({ en: "What is the OKX section for minting and trading NFTs?" }, 'NFT Marketplace'),
    makeEntry({ en: "Which OKX tool tracks portfolio balances across chains and networks?" }, 'Portfolio dashboard'),
    makeEntry({ en: "What OKX card allows spending crypto like cash?" }, 'OKX Card'),
    makeEntry({ en: "Which OKX feature automates grid-based buy and sell orders?" }, 'Grid trading bot'),
    makeEntry({ en: "Where do OKX users set API keys for programmatic access?" }, 'API management page'),
    makeEntry({ en: "Which OKX security feature allows hardware wallet connection for approvals?" }, 'Hardware wallet integration'),
    makeEntry({ en: "What OKX tab lists newly listed tokens and trading pairs?" }, 'New Listings'),
    makeEntry({ en: "Which community space shares OKX announcements and updates?" }, 'Announcements channel'),
    makeEntry({ en: "What setting in OKX Wallet enables transaction simulations before signing?" }, 'Transaction preview'),
    makeEntry({ en: "Which OKX feature offers dual-currency investments with fixed terms?" }, 'Dual Investment'),
    makeEntry({ en: "What OKX portal shows staking APRs across supported assets?" }, 'Staking hub'),
    makeEntry({ en: "Which OKX feature helps discover DeFi farms with boosted APY?" }, 'Yield farming aggregator'),
    makeEntry({ en: "Where can users check OKX platform status and incidents?" }, 'Status page'),
    makeEntry({ en: "Which OKX bot automates rebalancing around a reference price?" }, 'Martingale bot'),
    makeEntry({ en: "What OKX trading mode hides orders until they reach the book?" }, 'Iceberg order'),
    makeEntry({ en: "Which OKX module supports perpetual swap trading?" }, 'Futures/Perpetuals'),
    makeEntry({ en: "What tool on OKX helps convert small balances into OKB?" }, 'Convert dust to OKB'),
    makeEntry({ en: "Which OKX feature offers recurring crypto purchases on schedule?" }, 'Recurring buy'),
    makeEntry({ en: "Where do users adjust slippage tolerance for OKX DEX swaps?" }, 'Swap settings'),
    makeEntry({ en: "Which OKX product allows P2P fiat-to-crypto trades?" }, 'P2P trading desk'),
    makeEntry({ en: "What OKX section shows historical funding rates for perpetuals?" }, 'Funding rate history'),
    makeEntry({ en: "Which OKX wallet setting secures seed phrases with cloud backup encryption?" }, 'Cloud backup vault'),
    makeEntry({ en: "What OKX feature lets users follow strategy indices curated by the platform?" }, 'Index trading'),
    makeEntry({ en: "Where can users view OKX proof-of-reserves reports?" }, 'Proof of Reserves page'),
    makeEntry({ en: "Which OKX badge verifies ownership of specific NFTs or roles?" }, 'Verification badge'),
    makeEntry({ en: "What OKX module aggregates on-chain token prices and charts?" }, 'On-chain market data'),
    makeEntry({ en: "Which OKX tool alerts users when price crosses custom thresholds?" }, 'Price alerts'),
    makeEntry({ en: "What OKX feature allows lending out idle assets for interest?" }, 'Loan marketplace'),
    makeEntry({ en: "Which OKX program rewards referrals with commission?" }, 'Affiliate/Referral program'),
    makeEntry({ en: "What OKX setting enables passkeys or biometrics for wallet access?" }, 'Passwordless login'),
    makeEntry({ en: "Where can users manage address labels and whitelists on OKX?" }, 'Security settings'),
    makeEntry({ en: "Which OKX feature offers a launchpad for early token sales?" }, 'OKX Jumpstart'),
    makeEntry({ en: "What OKX function lets users testnet dApps safely?" }, 'Built-in testnet switcher'),
    makeEntry({ en: "Which OKX panel summarizes gas fees before confirming DEX swaps?" }, 'Fee preview panel'),
    makeEntry({ en: "What OKX experience provides quests for beginners to earn NFTs?" }, 'Learn-to-earn quests'),
    makeEntry({ en: "Which OKX feature routes large trades to reduce price impact?" }, 'Smart routing'),
    makeEntry({ en: "Where do OKX users find chain-specific RPC settings for the wallet?" }, 'Network management'),
    makeEntry({ en: "Which OKX tool scans approvals to revoke risky allowances?" }, 'Approval inspector'),
    makeEntry({ en: "What OKX option lets users lock OKB for privileges?" }, 'OKB lock & earn'),
    makeEntry({ en: "Which OKX service shows leaderboard rankings for traders?" }, 'Trader leaderboard'),
    makeEntry({ en: "Where can users view their check-in streak rewards on OKX?" }, 'Daily rewards center'),
    makeEntry({ en: "Which OKX bot keeps a fixed ratio between two assets?" }, 'Rebalancing bot'),
    makeEntry({ en: "What OKX feature protects orders from executing outside a price band?" }, 'Price protection'),
    makeEntry({ en: "Which OKX dashboard summarizes DEX swap history?" }, 'Swap history panel'),
    makeEntry({ en: "Where do users enable expert mode for advanced DEX parameters?" }, 'Advanced swap settings'),
    makeEntry({ en: "Which OKX product offers volatility-based options trading?" }, 'Options desk'),
    makeEntry({ en: "What OKX tool visualizes liquidation levels on the order book?" }, 'Liquidation heatmap'),
    makeEntry({ en: "Which OKX page provides tax export tools for trading history?" }, 'Tax report center'),
    makeEntry({ en: "What OKX function lets users set Take-Profit/Stop-Loss when opening trades?" }, 'TP/SL bracket orders'),
    makeEntry({ en: "Where can users monitor gas token balances for each chain in OKX Wallet?" }, 'Network asset view'),
    makeEntry({ en: "Which OKX feature allows fiat deposits via bank transfer or card?" }, 'Fiat deposit gateway'),
    makeEntry({ en: "What OKX module highlights trending NFT collections?" }, 'Trending NFTs tab'),
    makeEntry({ en: "Which OKX portal lists audits for listed projects?" }, 'Audit information page'),
    makeEntry({ en: "Where do users configure notification preferences in OKX app?" }, 'Notification center'),
    makeEntry({ en: "What OKX feature secures logins with rotating codes?" }, 'Two-factor authentication'),
    makeEntry({ en: "Which OKX dashboard shows margin ratios and risk levels?" }, 'Risk control panel'),
    makeEntry({ en: "What OKX module consolidates DeFi positions from multiple protocols?" }, 'DeFi position manager'),
    makeEntry({ en: "Where can users test strategies with virtual funds on OKX?" }, 'Demo trading mode'),
    makeEntry({ en: "Which OKX feature supports batch listing of NFTs for sale?" }, 'Bulk listing tool'),
    makeEntry({ en: "What OKX option lets users customize gas price manually?" }, 'Custom gas controls'),
    makeEntry({ en: "Which OKX service offers on-chain governance for community proposals?" }, 'Governance portal'),
    makeEntry({ en: "Where do users find tutorials for bridging to X Layer?" }, 'X Layer bridge guide'),
    makeEntry({ en: "Which OKX feature highlights ecosystem dApps built on X Layer?" }, 'X Layer dApp hub'),
    makeEntry({ en: "What OKX wallet view shows pending signature requests?" }, 'Signature request queue'),
    makeEntry({ en: "Which OKX function lets users claim airdrops routed to their wallet?" }, 'Airdrop claim center'),
    makeEntry({ en: "Where can users check validator info for OKX Chain?" }, 'Validator dashboard'),
    makeEntry({ en: "What OKX resource lists supported hardware wallet models?" }, 'Hardware compatibility list'),
    makeEntry({ en: "Which OKX feature notifies users about suspicious login attempts?" }, 'Login risk alerts'),
    makeEntry({ en: "What OKX portal tracks rebates from maker orders?" }, 'Fee rebate center'),
    makeEntry({ en: "Where do users manage sub-accounts and permissions in OKX?" }, 'Sub-account manager'),
    makeEntry({ en: "Which OKX setting enables spending limits for linked cards?" }, 'Card spending limits'),
    makeEntry({ en: "What OKX feature bundles multiple swaps into a single transaction?" }, 'Multi-hop swap'),
    makeEntry({ en: "Where can users see estimated bridge arrival times on OKX?" }, 'Bridge ETA tracker'),
    makeEntry({ en: "Which OKX module provides research reports and project analysis?" }, 'Research center'),
    makeEntry({ en: "What OKX feature groups favorite trading pairs for quick access?" }, 'Watchlist'),
    makeEntry({ en: "Where can users toggle night mode for OKX interfaces?" }, 'Appearance settings'),
    makeEntry({ en: "Which OKX tool estimates APR before joining a liquidity pool?" }, 'Pool APR estimator'),
    makeEntry({ en: "What OKX feature allows setting slippage separately for buys and sells?" }, 'Directional slippage control'),
    makeEntry({ en: "Where do users review smart contract interactions before signing?" }, 'Transaction details panel'),
    makeEntry({ en: "Which OKX mode hides balances when sharing screens?" }, 'Privacy mode'),
    makeEntry({ en: "What OKX filter shows dApps verified by security partners?" }, 'Trusted dApp filter'),
    makeEntry({ en: "Where can users request support directly within OKX Wallet?" }, 'In-app support chat'),
    makeEntry({ en: "Which OKX tracker displays historical swap routes used by the DEX?" }, 'Route history viewer'),
    makeEntry({ en: "What OKX option lets users favorite NFT creators?" }, 'Creator follow list'),
    makeEntry({ en: "Which OKX safety tool checks token metadata for risks?" }, 'Token risk scanner'),
    makeEntry({ en: "Where can users export private keys from OKX Web3 Wallet?" }, 'Export private key menu'),
];
const web3Questions = [
    makeEntry({ en: "What year did Bitcoin's whitepaper release to the public?" }, '2008'),
    makeEntry({ en: "Who created the Ethereum blockchain?" }, 'Vitalik Buterin'),
    makeEntry({ en: "Which consensus mechanism secures Ethereum after the Merge?" }, 'Proof of Stake'),
    makeEntry({ en: "What was the first block on Bitcoin called?" }, 'Genesis Block'),
    makeEntry({ en: "Which blockchain introduced smart contracts to the mainstream?" }, 'Ethereum'),
    makeEntry({ en: "What is the smallest unit of Bitcoin called?" }, 'Satoshi'),
    makeEntry({ en: "Which upgrade activated Taproot on Bitcoin?" }, 'BIP-341'),
    makeEntry({ en: "Who is credited with inventing proof-of-work for spam control?" }, 'Hal Finney'),
    makeEntry({ en: "Which network pioneered the UTXO model?" }, 'Bitcoin'),
    makeEntry({ en: "What does EVM stand for in blockchain technology?" }, 'Ethereum Virtual Machine'),
    makeEntry({ en: "Which blockchain popularized the concept of NFTs in 2021?" }, 'Ethereum'),
    makeEntry({ en: "What is the main purpose of a blockchain consensus algorithm?" }, 'To agree on the ledger state'),
    makeEntry({ en: "Which scaling solution rolls up transactions before posting to L1?" }, 'Optimistic rollup'),
    makeEntry({ en: "What does L2 stand for in blockchain architecture?" }, 'Layer 2'),
    makeEntry({ en: "Which chain uses the Beacon Chain for consensus?" }, 'Ethereum'),
    makeEntry({ en: "Who founded Polkadot and Kusama?" }, 'Gavin Wood'),
    makeEntry({ en: "Which mechanism burns ETH base fees after EIP-1559?" }, 'Fee burn'),
    makeEntry({ en: "What is the time between Ethereum blocks called?" }, 'Block time'),
    makeEntry({ en: "Which network introduced shards for parallel processing?" }, 'NEAR Protocol'),
    makeEntry({ en: "What was the first stablecoin widely used on exchanges?" }, 'Tether (USDT)'),
    makeEntry({ en: "Which consensus algorithm uses validators and slashing?" }, 'Proof of Stake'),
    makeEntry({ en: "Who authored the Web3 vision paper while at Ethereum?" }, 'Gavin Wood'),
    makeEntry({ en: "Which chain focuses on privacy with zk-SNARKs?" }, 'Zcash'),
    makeEntry({ en: "What is the native token of the Binance Smart Chain?" }, 'BNB'),
    makeEntry({ en: "Which protocol introduced automated market makers to DeFi?" }, 'Uniswap'),
    makeEntry({ en: "What blockchain is known for programmable UTXOs?" }, 'Cardano'),
    makeEntry({ en: "Which network uses Delegated Proof of Stake with 21 block producers?" }, 'EOS'),
    makeEntry({ en: "What is the maximum supply of Bitcoin?" }, '21 million'),
    makeEntry({ en: "Which year did Ethereum switch to Proof of Stake?" }, '2022'),
    makeEntry({ en: "What does DAO stand for?" }, 'Decentralized Autonomous Organization'),
    makeEntry({ en: "Which chain pioneered on-chain governance with referenda?" }, 'Tezos'),
    makeEntry({ en: "What is the native token of Solana?" }, 'SOL'),
    makeEntry({ en: "Which scaling tech uses zero-knowledge proofs for validity?" }, 'ZK-Rollup'),
    makeEntry({ en: "Who coined the term \"smart contracts\" in the 1990s?" }, 'Nick Szabo'),
    makeEntry({ en: "Which year saw the DeFi Summer boom?" }, '2020'),
    makeEntry({ en: "What does NFT stand for?" }, 'Non-Fungible Token'),
    makeEntry({ en: "Which blockchain hosts the CryptoPunks collection?" }, 'Ethereum'),
    makeEntry({ en: "What is the purpose of a blockchain oracle?" }, 'Provide off-chain data on-chain'),
    makeEntry({ en: "Which chain is famous for fast finality via PBFT-style consensus?" }, 'Tendermint-based chains'),
    makeEntry({ en: "Which network uses shards called parachains?" }, 'Polkadot'),
    makeEntry({ en: "What is the main function of a hardware wallet?" }, 'Offline key storage'),
    makeEntry({ en: "Which year did the DAO hack occur on Ethereum?" }, '2016'),
    makeEntry({ en: "What does MEV stand for?" }, 'Maximal Extractable Value'),
    makeEntry({ en: "Which protocol introduced bonding curves for token issuance?" }, 'Balancer'),
    makeEntry({ en: "What chain powers the BRC-20 token standard?" }, 'Bitcoin'),
    makeEntry({ en: "Which L2 uses fraud proofs and a challenge period?" }, 'Optimism'),
    makeEntry({ en: "What is the term for irreversible blockchain transactions?" }, 'Immutability'),
    makeEntry({ en: "Which blockchain uses ADA as its native token?" }, 'Cardano'),
    makeEntry({ en: "What is the purpose of gas fees on Ethereum?" }, 'Pay for computation'),
    makeEntry({ en: "Which chain uses validator epochs called eras?" }, 'Polkadot'),
    makeEntry({ en: "What type of wallets require multiple approvals to spend?" }, 'Multisig wallets'),
    makeEntry({ en: "Which chain is home to the Terra Luna Classic token after the fork?" }, 'Terra Classic'),
    makeEntry({ en: "What is the name of Ethereum's first hard fork post-DAO?" }, 'Ethereum Classic split'),
    makeEntry({ en: "Which blockchain introduced programmable privacy with viewing keys?" }, 'Secret Network'),
    makeEntry({ en: "What is the block reward for Bitcoin after the 2024 halving?" }, '3.125 BTC'),
    makeEntry({ en: "Which token standard defines fungible tokens on Ethereum?" }, 'ERC-20'),
    makeEntry({ en: "What is the goal of sharding in blockchains?" }, 'Increase throughput'),
    makeEntry({ en: "Which protocol brought liquid staking derivatives mainstream?" }, 'Lido'),
    makeEntry({ en: "What year did Binance launch its exchange?" }, '2017'),
    makeEntry({ en: "What is the time delay before Bitcoin transactions are considered final?" }, 'Confirmation time'),
    makeEntry({ en: "Which network built the Lightning Network for fast payments?" }, 'Bitcoin'),
    makeEntry({ en: "What does TVL measure in DeFi?" }, 'Total Value Locked'),
    makeEntry({ en: "Which chain uses the MOVE programming language?" }, 'Aptos'),
    makeEntry({ en: "Who introduced the concept of Merkle trees?" }, 'Ralph Merkle'),
    makeEntry({ en: "What is the role of a mempool in blockchain nodes?" }, 'Store pending transactions'),
    makeEntry({ en: "Which chain markets itself as the \"Internet Computer\"?" }, 'DFINITY\'s ICP'),
    makeEntry({ en: "What is a rug pull in crypto?" }, 'Developers drain liquidity and abandon a project'),
    makeEntry({ en: "Which protocol uses AMMs to trade synthetic assets?" }, 'Synthetix'),
    makeEntry({ en: "What is the purpose of slashing in PoS systems?" }, 'Penalize malicious validators'),
    makeEntry({ en: "Which blockchain hosts the ENS naming service?" }, 'Ethereum'),
    makeEntry({ en: "What is the process of locking tokens to support network security?" }, 'Staking'),
    makeEntry({ en: "Which network deploys canisters instead of smart contracts?" }, 'Internet Computer'),
    makeEntry({ en: "What is the primary benefit of zk-SNARKs?" }, 'Proving validity without revealing data'),
    makeEntry({ en: "Which blockchain introduced the concept of sidechains via drivechains?" }, 'Rootstock (RSK)'),
    makeEntry({ en: "What is the main risk mitigated by hardware wallets?" }, 'Online key theft'),
    makeEntry({ en: "Which year did Mt. Gox collapse?" }, '2014'),
    makeEntry({ en: "What does DEX stand for?" }, 'Decentralized Exchange'),
    makeEntry({ en: "What is the purpose of a genesis block?" }, 'Initialize the ledger'),
    makeEntry({ en: "Which chain uses gas unit called gwei?" }, 'Ethereum'),
    makeEntry({ en: "What type of consensus does Ripple use?" }, 'Byzantine agreement via validators'),
    makeEntry({ en: "Which blockchain is known for fast block times using Proof of History?" }, 'Solana'),
    makeEntry({ en: "What is the economic role of miners or validators?" }, 'Secure the network for rewards'),
    makeEntry({ en: "Which upgrade merged Ethereum's execution and consensus layers?" }, 'The Merge'),
    makeEntry({ en: "What does hash rate represent?" }, 'Total computational power on a network'),
    makeEntry({ en: "Which chain relies on masternodes for governance and mixing?" }, 'Dash'),
    makeEntry({ en: "What is the smallest unit of Ether?" }, 'Wei'),
    makeEntry({ en: "Which year saw the launch of Uniswap v1?" }, '2018'),
    makeEntry({ en: "What is a block explorer used for?" }, 'Viewing on-chain transactions'),
    makeEntry({ en: "Which chain introduced gas tokens like Chi and GST2?" }, 'Ethereum'),
    makeEntry({ en: "What does HODL originally mean in crypto slang?" }, 'Hold without selling'),
    makeEntry({ en: "Which project launched the first widely used blockchain oracle?" }, 'Chainlink'),
    makeEntry({ en: "What is the purpose of a testnet?" }, 'Trial deployments without real funds'),
    makeEntry({ en: "Which chain focuses on interoperable smart contracts via Cosmos SDK?" }, 'Cosmos chains'),
    makeEntry({ en: "What mechanism keeps stablecoins like DAI near $1?" }, 'Collateralized debt positions'),
    makeEntry({ en: "Which year did Bitcoin reach parity with the US dollar?" }, '2011'),
    makeEntry({ en: "What does ECDSA provide for blockchain addresses?" }, 'Digital signatures'),
    makeEntry({ en: "Which network pioneered liquid democracy with bakers?" }, 'Tezos'),
    makeEntry({ en: "What is the main benefit of multisig treasury management?" }, 'Shared control over funds'),
    makeEntry({ en: "Which protocol popularized yield farming incentives?" }, 'Compound'),
    makeEntry({ en: "What does interoperability mean in Web3?" }, 'Cross-chain communication'),
];

module.exports = {
    SCIENCE_LANGS: SUPPORTED_LANGS,
    SCIENCE_TEMPLATES,
    SCIENCE_ENTRIES: {
        physics: physicsConcepts,
        chemistry: chemistryConcepts,
        okx: okxQuestions,
        web3: web3Questions
    }
};