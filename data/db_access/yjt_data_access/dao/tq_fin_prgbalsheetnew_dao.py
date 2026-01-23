from data.db_access.mysql_base_dao import MySQLDao
from data.db_access.yjt_data_access.entity.tq_fin_prgbalsheetnew import TqfinPrgbalsheetnew

class TqfinPrgbalsheetnewDao(MySQLDao):

    def select_sheet(self, com_code):
        query = """
        select 
            t.ENDDATE
            ,round(t.CURFDS          /10000,2) CURFDS
            ,round(t.SETTRESEDEPO    /10000,2) SETTRESEDEPO
            ,round(t.PLAC            /10000,2) PLAC
            ,round(t.TRADFINASSET    /10000,2) TRADFINASSET
            ,round(t.DERIFINAASSET   /10000,2) DERIFINAASSET
            ,round(t.NOTESRECE       /10000,2) NOTESRECE
            ,round(t.ACCORECE        /10000,2) ACCORECE
            ,round(t.PREP            /10000,2) PREP
            ,round(t.PREMRECE        /10000,2) PREMRECE
            ,round(t.REINRECE        /10000,2) REINRECE
            ,round(t.REINCONTRESE    /10000,2) REINCONTRESE
            ,round(t.INTERECE        /10000,2) INTERECE
            ,round(t.DIVIDRECE       /10000,2) DIVIDRECE
            ,round(t.OTHERRECE       /10000,2) OTHERRECE
            ,round(t.EXPOTAXREBARECE /10000,2) EXPOTAXREBARECE
            ,round(t.SUBSRECE        /10000,2) SUBSRECE
            ,round(t.MARGRECE        /10000,2) MARGRECE
            ,round(t.INTELRECE       /10000,2) INTELRECE
            ,round(t.PURCRESAASSET   /10000,2) PURCRESAASSET
            ,round(t.INVE            /10000,2) INVE
            ,round(t.PREPEXPE        /10000,2) PREPEXPE
            ,round(t.UNSEG           /10000,2) UNSEG
            ,round(t.EXPINONCURRASSET/10000,2) EXPINONCURRASSET
            ,round(t.OTHERCURRASSE   /10000,2) OTHERCURRASSE
            ,round(t.TOTCURRASSET    /10000,2) TOTCURRASSET
            ,round(t.LENDANDLOAN     /10000,2) LENDANDLOAN
            ,round(t.AVAISELLASSE    /10000,2) AVAISELLASSE
            ,round(t.HOLDINVEDUE      /10000,2) HOLDINVEDUE
            ,round(t.LONGRECE        /10000,2) LONGRECE
            ,round(t.EQUIINVE        /10000,2) EQUIINVE
            ,round(t.OTHERLONGINVE   /10000,2) OTHERLONGINVE
            ,round(t.INVEPROP        /10000,2) INVEPROP
            ,round(t.FIXEDASSEIMMO   /10000,2) FIXEDASSEIMMO
            ,round(t.ACCUDEPR        /10000,2) ACCUDEPR
            ,round(t.FIXEDASSENETW   /10000,2) FIXEDASSENETW
            ,round(t.FIXEDASSEIMPA   /10000,2) FIXEDASSEIMPA
            ,round(t.FIXEDASSENET    /10000,2) FIXEDASSENET
            ,round(t.CONSPROG        /10000,2) CONSPROG
            ,round(t.ENGIMATE        /10000,2) ENGIMATE
            ,round(t.FIXEDASSECLEA   /10000,2) FIXEDASSECLEA
            ,round(t.PRODASSE        /10000,2) PRODASSE
            ,round(t.COMASSE         /10000,2) COMASSE
            ,round(t.HYDRASSET       /10000,2) HYDRASSET
            ,round(t.INTAASSET       /10000,2) INTAASSET
            ,round(t.DEVEEXPE        /10000,2) DEVEEXPE
            ,round(t.GOODWILL        /10000,2) GOODWILL
            ,round(t.LOGPREPEXPE     /10000,2) LOGPREPEXPE
            ,round(t.TRADSHARTRAD    /10000,2) TRADSHARTRAD
            ,round(t.DEFETAXASSET    /10000,2) DEFETAXASSET
            ,round(t.OTHERNONCASSE /10000,2) OTHERNONCASSE
            ,round(t.TOTALNONCASSETS /10000,2) TOTALNONCASSETS
            ,round(t.TOTASSET        /10000,2) TOTASSET
            ,round(t.SHORTTERMBORR   /10000,2) SHORTTERMBORR
            ,round(t.CENBANKBORR     /10000,2) CENBANKBORR
            ,round(t.DEPOSIT         /10000,2) DEPOSIT
            ,round(t.FDSBORR         /10000,2) FDSBORR
            ,round(t.TRADFINLIAB     /10000,2) TRADFINLIAB
            ,round(t.DERILIAB        /10000,2) DERILIAB
            ,round(t.NOTESPAYA       /10000,2) NOTESPAYA
            ,round(t.ACCOPAYA        /10000,2) ACCOPAYA
            ,round(t.ADVAPAYM        /10000,2) ADVAPAYM
            ,round(t.SELLREPASSE     /10000,2) SELLREPASSE
            ,round(t.COPEPOUN        /10000,2) COPEPOUN
            ,round(t.COPEWORKERSAL   /10000,2) COPEWORKERSAL
            ,round(t.TAXESPAYA       /10000,2) TAXESPAYA
            ,round(t.INTEPAYA        /10000,2) INTEPAYA
            ,round(t.DIVIPAYA        /10000,2) DIVIPAYA
            ,round(t.OTHERFEEPAYA    /10000,2) OTHERFEEPAYA
            ,round(t.MARGREQU        /10000,2) MARGREQU
            ,round(t.INTELPAY        /10000,2) INTELPAY
            ,round(t.OTHERPAY        /10000,2) OTHERPAY
            ,round(t.ACCREXPE        /10000,2) ACCREXPE
            ,round(t.EXPECURRLIAB    /10000,2) EXPECURRLIAB
            ,round(t.COPEWITHREINRECE/10000,2) COPEWITHREINRECE
            ,round(t.INSUCONTRESE    /10000,2) INSUCONTRESE
            ,round(t.ACTITRADSECU    /10000,2) ACTITRADSECU
            ,round(t.ACTIUNDESECU    /10000,2) ACTIUNDESECU
            ,round(t.INTETICKSETT     /10000,2) INTETICKSETT
            ,round(t.DOMETICKSETT    /10000,2) DOMETICKSETT
            ,round(t.DEFEREVE        /10000,2) DEFEREVE
            ,round(t.SHORTTERMBDSPAYA/10000,2) SHORTTERMBDSPAYA
            ,round(t.DUENONCLIAB     /10000,2) DUENONCLIAB
            ,round(t.OTHERCURRELIABI  /10000,2) OTHERCURRELIABI
            ,round(t.TOTALCURRLIAB   /10000,2) TOTALCURRLIAB
            ,round(t.LONGBORR        /10000,2) LONGBORR
            ,round(t.BDSPAYA         /10000,2) BDSPAYA
            ,round(t.LONGPAYA        /10000,2) LONGPAYA
            ,round(t.SPECPAYA        /10000,2) SPECPAYA
            ,round(t.EXPENONCLIAB    /10000,2) EXPENONCLIAB
            ,round(t.LONGDEFEINCO    /10000,2) LONGDEFEINCO
            ,round(t.DEFEINCOTAXLIAB /10000,2) DEFEINCOTAXLIAB
            ,round(t.OTHERNONCLIABI  /10000,2) OTHERNONCLIABI
            ,round(t.TOTALNONCLIAB   /10000,2) TOTALNONCLIAB
            ,round(t.TOTLIAB         /10000,2) TOTLIAB
            ,round(t.PAIDINCAPI      /10000,2) PAIDINCAPI
            ,round(t.CAPISURP       /10000,2) CAPISURP
            ,round(t.TREASTK         /10000,2) TREASTK
            ,round(t.SPECRESE        /10000,2) SPECRESE
            ,round(t.RESE           /10000,2) RESE
            ,round(t.GENERISKRESE    /10000,2) GENERISKRESE
            ,round(t.UNREINVELOSS    /10000,2) UNREINVELOSS
            ,round(t.UNDIPROF        /10000,2) UNDIPROF
            ,round(t.TOPAYCASHDIVI   /10000,2) TOPAYCASHDIVI
            ,round(t.PARESHARRIGH    /10000,2) PARESHARRIGH
            ,round(t.MINYSHARRIGH    /10000,2) MINYSHARRIGH
            ,round(t.RIGHAGGR        /10000,2) RIGHAGGR
            ,round(t.TOTLIABSHAREQUI /10000,2) TOTLIABSHAREQUI
            ,round(t.REPORTYEAR      /10000,2) REPORTYEAR
            ,round(t.WARLIABRESE     /10000,2) WARLIABRESE
            ,round(t.LCOPEWORKERSAL  /10000,2) LCOPEWORKERSAL
            ,round(t.LIABHELDFORS    /10000,2) LIABHELDFORS
            ,round(t.ACCHELDFORS     /10000,2) ACCHELDFORS
            ,round(t.PERBOND         /10000,2) PERBOND
            ,round(t.PREST           /10000,2) PREST
            ,round(t.OTHEQUIN        /10000,2) OTHEQUIN
            ,round(t.OCL             /10000,2) OCL
            ,round(t.BDSPAYAPREST    /10000,2) BDSPAYAPREST
            ,round(t.BDSPAYAPERBOND  /10000,2) BDSPAYAPERBOND
            ,round(t.NOTESACCORECE   /10000,2) NOTESACCORECE
            ,round(t.CONTRACTASSET   /10000,2) CONTRACTASSET
            ,round(t.OTHDEBTINVEST   /10000,2) OTHDEBTINVEST
            ,round(t.OTHEQUININVEST  /10000,2) OTHEQUININVEST
            ,round(t.OTHERNONCFINASSE/10000,2) OTHERNONCFINASSE
            ,round(t.NOTESACCOPAYA   /10000,2) NOTESACCOPAYA
            ,round(t.CONTRACTLIAB    /10000,2) CONTRACTLIAB
            ,round(t.FAIRVALUEASSETS /10000,2) FAIRVALUEASSETS
            ,round(t.AMORTIZCOSTASSETS/10000,2) AMORTIZCOSTASSETS
            ,round(t.FIXEDASSECLEATOT /10000,2) FIXEDASSECLEATOT
            ,round(t.CONSPROGTOT    /10000,2) CONSPROGTOT
            ,round(t.LONGPAYATOT     /10000,2) LONGPAYATOT
            ,round(t.OTHERRECETOT   /10000,2) OTHERRECETOT
            ,round(t.OTHERPAYTOT     /10000,2) OTHERPAYTOT
            ,round(t.RECFINANC      /10000,2) RECFINANC
            ,round(t.RUSEASSETS      /10000,2) RUSEASSETS
            ,round(t.LEASELIAB       /10000,2) LEASELIAB
        from (
            select t1.*
                  ,row_number() over(partition by t1.REPORTYEAR order by t1.ENDDATE desc) as row_num
            from TQ_FIN_PRGBALSHEETNEW t1
            join (
                select distinct REPORTYEAR
                from TQ_FIN_PRGBALSHEETNEW
                where REPORTTYPE='1' and COMPCODE=:comCode
                order by REPORTYEAR desc
                limit 3
            ) t2
              on t1.REPORTTYPE='1' and t1.COMPCODE=:comCode and t1.REPORTYEAR=t2.REPORTYEAR
        ) t
        where t.row_num = 1
        """

        params = {"comCode": com_code}
        result = self.select_list_by_sql(query, params, TqfinPrgbalsheetnew)
        return result