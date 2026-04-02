================================================================================
  AGGRESSIVENESS COEFFICIENT V2 — Calcolo dettagliato per metrica
  Script: AggV2.py  |  Preparazione dati: build_aggressiveness_coeff.py
================================================================================

LOGICA GENERALE
---------------
Per ogni giocatore dei top-200 (file scrapers/top200.txt) vengono calcolati
indici percentili (0-100) rispetto all'intera popolazione dei 200 giocatori.

  100 = il giocatore e nel top della distribuzione
    0 = e in fondo alla distribuzione
   50 = e esattamente nella media

Il processo ha due fasi:

  FASE 1 — build_aggressiveness_coeff.py
    Legge tennis_abstract_merged.db e calcola per ogni giocatore il valore
    grezzo numerico di ciascuna metrica. Lo salva in aggressiveness_coeff.db,
    tabella glossary_top200.

  FASE 2 — AggV2.py
    Legge glossary_top200, trasforma ogni valore grezzo in percentile usando
    scipy.stats.percentileofscore(..., kind="mean"), aggrega per area tematica
    e salva il risultato in aggressiveness_v2.db, tabella aggressiveness_index.


================================================================================
COME VIENE ESTRATTO IL VALORE GREZZO DI CIASCUNA METRICA
================================================================================

Esistono tre "kind" di sorgente:

  group_row
    Legge una singola riga da una tabella aggregata filtrata per
    row_column = row_value (tipicamente Year = "Career").
    Il valore e quello della riga Career del giocatore, quindi
    rappresenta la statistica di tutta la carriera.

  group_average
    Legge tutte le righe disponibili del giocatore nella tabella
    e ne calcola la media aritmetica. Usato quando non c'e una
    riga "Career" preagregata, oppure per statistiche charting
    che hanno una riga per match o per stagione.

  mcp_comparison
    Legge la media di una colonna dalla tabella
    mcp_m_stats_shotdirection_enriched, filtrando per nome giocatore.
    Usato per le direzioni del colpo (crosscourt, down_the_line, ecc.)
    provenienti dal Match Charting Project.


================================================================================
METRICHE — DETTAGLIO PER AREA
================================================================================

--------------------------------------------------------------------------------
SERVE (7 metriche)  |  Tabella sorgente: group_004
--------------------------------------------------------------------------------

  group_004 corrisponde alle tabelle {Player}_Career_TourLevel_SplitsTop
  (o ChallengerLevel per i giocatori Challenger-only).
  Il valore estratto e sempre la riga con Year = "Career", che rappresenta
  la statistica aggregata su tutta la carriera.

  METRICA          COD.       POLARITA   VALORE GREZZO
  -------          ----       --------   -------------
  1sts %           1stIn         +1      % di prime di servizio messe in campo
                                         Es. 65.1 => 65.1% di prime in campo

  1st won %        1st%          +1      % di punti vinti quando la prima e in
                                         Es. 73.3 => vince il 73.3% con la 1a

  2nd won %        2nd%          +1      % di punti vinti con la seconda
                                         Es. 57.0 => vince il 57.0% con la 2a

  Aces %           A%            +1      Ace per punto di servizio (%)
                                         Es. 6.3 => 6.3 ace ogni 100 punti serviti

  Serve hold %     Hld%          +1      % di giochi al servizio tenuti (hold)
                                         Es. 86.1 => tiene l'86.1% dei propri game

  Serve Pts won    SPW           +1      % totale di punti al servizio vinti
                                         (media pesata su prima e seconda)

  Double Faults %  DF%           -1      % di doppi falli per punto di servizio
                                         Polarita -1: percentile invertito,
                                         chi ne fa meno ottiene score piu alto.


--------------------------------------------------------------------------------
RALLY (5 metriche)  |  Tabelle: group_008, group_016
--------------------------------------------------------------------------------

  group_008 corrisponde a {Player}_Winners_and_Unforced_Errors_ContextTop
  I valori vengono mediati su tutte le righe disponibili (group_average).

  group_016 corrisponde a dati Match Charting Project (rally stats).
  Anche qui si usa group_average.

  METRICA              COD.         POL.   SORGENTE   VALORE GREZZO
  -------              ----         ----   --------   -------------
  Unforced Errors      UFE/Pt        -1    group_008  Errori non forzati per punto
                                                      Polarita -1: chi ne fa meno
                                                      ottiene score piu alto.

  Rally Aggressiveness RallyAgg      +1    group_016  Indice di aggressivita in scambio
                                                      dal Match Charting Project.
                                                      ** Valore scalato (vedi sotto) **

  Rally Winners/UFE    Ratio         +1    group_008  Rapporto winners-per-punto /
                                                      errori-non-forzati-per-punto.
                                                      Es. 1.26 => fa piu winner che UFE

  Forehand Win %       FH_Wnr/Pt     +1    group_008  Winner di dritto per punto (%)

  Backhand Win %       BH_Wnr/Pt     +1    group_008  Winner di rovescio per punto (%)

  >> SCALING RALLYAGG <<
     Il valore grezzo di RallyAgg non e una percentuale ma un indice continuo.
     Prima di calcolare il percentile viene trasformato in una banda [0.5, 1.5]
     centrata sulla media ATP (= 1.0) con questa logica:

       se x >= media_ATP:  scaled = 1.0 + 0.5 * (x - media_ATP) / (p90 - media_ATP)
       se x <  media_ATP:  scaled = 1.0 - 0.5 * (media_ATP - x) / (media_ATP - p10)

     dove p10 e p90 sono il 10° e 90° percentile della distribuzione dei 200.
     Il risultato viene poi clampato in [0.5, 1.5].
     Questa trasformazione garantisce che la distribuzione sia simmetrica
     attorno alla media e non distorta da outlier.


--------------------------------------------------------------------------------
ATTITUDE (7 metriche)  |  Tabelle: group_011, group_016, group_004
--------------------------------------------------------------------------------

  group_011 corrisponde a {Player}_Key_GamesGlossaryTop o Key_PointsGlossaryTop.
  I valori vengono mediati su tutte le righe disponibili (group_average).

  group_016 = Match Charting (vedi Rally sopra).
  group_004 = Career Splits, riga Career (vedi Serve sopra).

  METRICA              COD.           POL.   SORGENTE   VALORE GREZZO
  -------              ----           ----   --------   -------------
  Serve Stay/Match     SvStayMatch     +1    group_011  % di giochi al servizio vinti
                                                        quando si e sotto nel punteggio
                                                        e si serve per restare in partita.

  Return Aggressiveness ReturnAgg      +1    group_016  Indice di aggressivita in risposta
                                                        dal Match Charting Project.
                                                        (non scalato come RallyAgg)

  Rally Aggressiveness  RallyAgg       +1    group_016  Stesso valore scalato descritto
                                                        nella sezione Rally qui sopra.

  Break Consol. %       Consol%        +1    group_011  % di giochi al servizio vinti
                                                        immediatamente dopo aver strappato
                                                        il break all'avversario.

  Tie Break won         TB%            +1    group_004  % di tiebreak vinti in carriera.
                                                        Estratto dalla riga Career.

  Break Back %          BreakBack%     +1    group_011  % di contro-break immediato dopo
                                                        aver perso il proprio servizio.

  Break Pts converted   BP_Conv        +1    group_010  % di palle break convertite.
                                                        group_010 = {Player}_Key_Points
                                                        o BreakPoint stats.
                                                        Valore mediato (group_average).


--------------------------------------------------------------------------------
TACTICS (7 metriche — 2 neutre, 5 attive)  |  Tabelle: group_016, mcp_direction
--------------------------------------------------------------------------------

  group_016 = Match Charting (vedi Rally).
  mcp_m_stats_shotdirection_enriched = tabella direzione colpi dal Match
  Charting Project, una riga per partita/match. Il valore usato e la media
  di tutte le partite disponibili per il giocatore (mcp_comparison).

  METRICA              COD.           POL.   SORGENTE        VALORE GREZZO
  -------              ----           ----   --------        -------------
  Drop Freq.           Drop:_Freq      0     group_016       Frequenza drop shot per scambio.
                                                             Polarita 0 = NON inclusa nel
                                                             calcolo del coeff_tactics.

  Net Freq.            Net_Freq        +1    group_016       Frequenza avanzate a rete.

  Serve & Volley Freq. SnV_Freq        +1    group_016       Frequenza serve & volley.

  Crosscourt %         crosscourt      0     mcp_direction   % colpi giocati in diagonale.
                                                             Polarita 0 = NON inclusa nel
                                                             calcolo del coeff_tactics.

  Down the Line %      down_the_line   +1    mcp_direction   % colpi giocati lungo linea.

  Inside In %          inside_in       +1    mcp_direction   % colpi inside-in
                                                             (dritto dal lato dritto).

  Inside Out %         inside_out      +1    mcp_direction   % colpi inside-out
                                                             (dritto dal lato rovescio).

  Nota: Drop e Crosscourt hanno polarita 0 perche non hanno una direzione
  "buona" o "cattiva" univoca — un drop shot frequente puo essere un punto
  di forza o una debolezza, dipende dal contesto. Sono salvati nel DB
  ma non influenzano coeff_tactics.


--------------------------------------------------------------------------------
EFFICIENCY (7 metriche)  |  Tabelle: group_008, group_004, group_011
--------------------------------------------------------------------------------

  Questa area e parzialmente sovrapposta con Serve e Attitude perche misura
  la capacita globale di convertire il gioco in punti e game.

  METRICA              COD.           POL.   SORGENTE   VALORE GREZZO
  -------              ----           ----   --------   -------------
  Ratio W/UE           Ratio           +1    group_008  Rapporto winners/UFE (come in Rally)

  Return Pts won       RPW             +1    group_004  % totale punti in risposta vinti.
                                                        Estratto dalla riga Career.

  Winners              Wnr/Pt          +1    group_008  Winner per punto (%)

  Serve Pts won        SPW             +1    group_004  % punti al servizio vinti (Career).
                                                        Stessa metrica di Serve | SPW.

  Serve hold %         Hld%            +1    group_004  % giochi al servizio tenuti (Career).
                                                        Stessa metrica di Serve | Hld%.

  Break vs Break Pts   BP_Conv/BPG     +1    group_011  Break point convertiti diviso
                                                        numero di giochi in cui si hanno
                                                        palle break. Misura l'efficienza
                                                        nel sfruttare le opportunita.

  Games w/ Break Pts   BP_Games        +1    group_011  % di game in cui il giocatore
                                                        crea almeno una palla break.
                                                        Misura la capacita di pressare
                                                        il servizio avversario.


================================================================================
INDICI PER SUPERFICIE  |  Tabella: group_007 in tennis_abstract_merged.db
================================================================================

  FONTE DATI
  ----------
  group_007 corrisponde alle tabelle {Player}_Career_TourLevel_SplitsTop
  presenti nel DB raw. Per i giocatori che giocano principalmente a livello
  Challenger (nessuna TourLevel_Splits disponibile), viene usata in automatico
  la tabella {Player}_Career_ChallengerLevel_SplitsTop come fallback.

  La tabella contiene una riga per ogni tipo di split (Hard, Clay, Grass,
  Grand Slams, Masters, Best of 3, Finals, ecc.). Vengono usate solo le
  righe con Split IN ('Hard', 'Clay', 'Grass').

  Le colonne lette per ciascuna superficie sono:
    "M"     numero totale di match giocati su quella superficie (career)
    "Win%"  percentuale di match vinti su quella superficie (career)

  CALCOLO DEI COEFFICIENTI INDIVIDUALI (coeff_hard, coeff_clay, coeff_grass)
  ---------------------------------------------------------------------------
  Per ciascuna delle tre superfici il processo e identico a quello delle
  altre metriche:

    1. Si legge il valore grezzo Win% di carriera del giocatore.
       Es. Alcaraz su Clay: 84.5%

    2. Si calcola il percentile del giocatore rispetto agli altri top-200
       che hanno dati su quella superficie, usando percentileofscore
       con kind="mean".
       Es. se la Win% di Alcaraz e piu alta del 99.2% degli altri => coeff_clay = 99.2

    3. La distribuzione di riferimento e composta solo dai giocatori top-200
       (stessa popolazione di tutti gli altri coefficienti).

    4. Se un giocatore non ha dati per una superficie (es. nessun match su
       erba nel dataset), il valore viene impostato a 50.0 (neutro).

  CALCOLO DI coeff_surface (media pesata per numero di match)
  -----------------------------------------------------------
  coeff_hard, coeff_clay e coeff_grass NON vengono inclusi direttamente
  nel global con peso uguale, perche cio penalizzerebbe i giocatori
  specializzati su una sola superficie.

  Viene invece calcolato un unico indice coeff_surface come media pesata
  per numero di match giocati:

    coeff_surface = (Hard_M * coeff_hard + Clay_M * coeff_clay + Grass_M * coeff_grass)
                    / (Hard_M + Clay_M + Grass_M)

  Il peso di ciascuna superficie e proporzionale a quanti match il giocatore
  ha effettivamente giocato su di essa. Cosi un giocatore che disputa il 70%
  dei propri match su Hard vedra il suo coeff_surface determinato
  prevalentemente dalla sua Win% su Hard.

  Esempio — Jacob Fearnley (Hard specialist):
    Hard_M=26  coeff_hard=21.1  =>  26 * 21.1  =  548.6
    Clay_M=12  coeff_clay=80.2  =>  12 * 80.2  =  962.4
    Grass_M=10 coeff_grass=40.2 =>  10 * 40.2  =  402.0
    Totale match = 48
    coeff_surface = (548.6 + 962.4 + 402.0) / 48 = 39.9

  Il suo buon risultato su Clay (80.2) viene ridimensionato dal peso
  maggiore dei match su Hard (dove vince solo il 34.6%).

  Se un giocatore non ha dati per nessuna superficie, coeff_surface
  viene calcolato come media semplice dei tre coeff (default 50.0 ciascuno).

  COLONNE NEL DB OUTPUT
  ---------------------
  hard_M          numero di match giocati su cemento (grezzo, non percentile)
  clay_M          numero di match giocati su terra   (grezzo)
  grass_M         numero di match giocati su erba    (grezzo)
  coeff_hard      percentile Win% su cemento (0-100)
  coeff_clay      percentile Win% su terra   (0-100)
  coeff_grass     percentile Win% su erba    (0-100)
  coeff_surface   media pesata per match dei tre coeff (0-100) — entra nel global


================================================================================
INDICE LUNGHEZZA SCAMBIO  |  Tabella: *_Match_Charting_Project_Rally* nel DB raw
================================================================================

  FONTE DATI
  ----------
  Per ogni giocatore viene cercata la tabella
  {player_key}_Match_Charting_Project_Rally* nel DB raw.
  Viene preferita la variante "RallyAll_matchesGlossary" (tutte le partite),
  poi "RallyGlossary", poi la tabella piu lunga disponibile.
  Il valore letto e sempre la riga con Match LIKE 'Career%', che rappresenta
  la statistica aggregata su tutta la carriera.

  METRICHE GREZZE
  ---------------
  1-3_W%    % di punti vinti negli scambi da 1 a 3 colpi  (primo attacco)
  4-6_W%    % di punti vinti negli scambi da 4 a 6 colpi  (scambio corto)
  7-9_W%    % di punti vinti negli scambi da 7 a 9 colpi  (scambio medio-lungo)
  10+_W%    % di punti vinti negli scambi da 10+ colpi    (grinder)

  CALCOLO DEI PERCENTILI
  ----------------------
  Per ognuna delle 4 fasce, il valore grezzo viene trasformato in percentile
  con lo stesso metodo percentileofscore(..., kind="mean") usato per tutte
  le altre metriche. La distribuzione di riferimento e quella dei top-200.
  Se un giocatore non ha dati (nessuna tabella Rally disponibile), il
  percentile viene impostato a 50.0 (neutro).

  Le colonne nel DB output sono:
    pct_1-3_W%    percentile su scambi 1-3 colpi (0-100)
    pct_4-6_W%    percentile su scambi 4-6 colpi (0-100)
    pct_7-9_W%    percentile su scambi 7-9 colpi (0-100)
    pct_10+_W%    percentile su scambi 10+ colpi (0-100)

  CALCOLO DI coeff_rally_length (media pesata)
  --------------------------------------------
  I 4 percentili vengono combinati con pesi decrescenti per valorizzare
  il gioco aggressivo di primo attacco rispetto alla resistenza fisica.

  I pesi sono definiti dalla costante RALLY_LENGTH_WEIGHTS in cima ad
  AggV2.py e possono essere modificati liberamente. I valori attuali sono:

    1-3_W%  =>  peso 0.40  (primo attacco / servizio diretto)
    4-6_W%  =>  peso 0.30  (scambio corto)
    7-9_W%  =>  peso 0.20  (scambio medio-lungo)
    10+_W%  =>  peso 0.10  (grinder / resistenza)

  I pesi devono sommare a 1.0.

  Formula:
    coeff_rally_length = 0.40 * pct(1-3) + 0.30 * pct(4-6)
                       + 0.20 * pct(7-9) + 0.10 * pct(10+)

  Esempio — un giocatore con percentili pct(1-3)=90, pct(4-6)=70,
  pct(7-9)=50, pct(10+)=30:
    coeff_rally_length = 0.40*90 + 0.30*70 + 0.20*50 + 0.10*30
                       = 36 + 21 + 10 + 3 = 70.0

  COME CAMBIARE I PESI
  --------------------
  Aprire AggV2.py e modificare il dizionario RALLY_LENGTH_WEIGHTS:

    RALLY_LENGTH_WEIGHTS: dict[str, float] = {
        "1-3_W%":  0.40,
        "4-6_W%":  0.30,
        "7-9_W%":  0.20,
        "10+_W%":  0.10,
    }

  Assicurarsi che i 4 valori sommino sempre a 1.0, poi rieseguire AggV2.py.


================================================================================
AGGREGAZIONE FINALE
================================================================================

  coeff_serve         = media dei percentili delle 7 metriche Serve   (tutte attive)
  coeff_rally         = media dei percentili delle 5 metriche Rally   (tutte attive)
  coeff_attitude      = media dei percentili delle 7 metriche Attitude (tutte attive)
  coeff_tactics       = media dei percentili delle 5 metriche attive Tactics
                        (Drop e Crosscourt escluse perche polarita = 0)
  coeff_efficiency    = media dei percentili delle 7 metriche Efficiency
  coeff_surface       = media pesata per match di coeff_hard/clay/grass (vedi sopra)
  coeff_rally_length  = media pesata dei percentili 1-3/4-6/7-9/10+ (vedi sopra)

  coeff_global        = media semplice dei 7 topic score
                      = (coeff_serve + coeff_rally + coeff_attitude
                         + coeff_tactics + coeff_efficiency
                         + coeff_surface + coeff_rally_length) / 7

  rank_aggressiveness = posizione nella classifica dei 200 per coeff_global


================================================================================
ESECUZIONE
================================================================================

  Dal root del progetto:

    python Agg_Coeff\build_aggressiveness_coeff.py   # prepara glossary_top200
    python Agg_Coeff\AggV2.py                        # calcola tutti gli indici

  Output: Agg_Coeff\aggressiveness_v2.db
          tabella: aggressiveness_index (200 righe)

================================================================================
