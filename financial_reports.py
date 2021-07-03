''' 
Projeto: Gerar relatórios consolidados a partir de um extrato de negociações, atualizar o fluxo de caixa e enviar email com os resultados.
Problema: Ler aruivo de extrato de negociaçãos, fazer relatórios sobre as operações do dia, mês e ano, atualizar fluxo de caixa e enviar email com os anexos a uma lista de pessoas, sendo possível adicionar mais 1 anexo para cada pessoa.
Python 3.8.3

Passos do algoritmo:
1. Importar os pacotes necessário 
2. Abrir o arquivo do extrato como dataframe
3. Selecionar colunas e alterar tipo 
4. Criar novas colunas relevantes para a elaboração do relatório
5. Gerar relatórios
6. Criar fluxo de caixa atualizado
7. Gerar planilha com impostos a pagar sobre os valores ganhos, Separar por ano,  day trade ou nao, ação, opção e mercado futuro dividindo alíquotas para day trade e normal.
8. Ler usuário e senha do remetente
9. Solicitar se quer adicionar outro arquivo e verificar que os arquivos são em excel 
10.Ler lista de destinatários e enviar email


'''


# Imports
import pandas as pd
import numpy as np
import datetime
import re
import smtplib, ssl
from email.message import EmailMessage


#helper functions
def get_data(path):
    data = pd.read_excel('./database/ReportHistory.xlsx', header=5, engine='openpyxl')
    data.rename(columns=data.iloc[0], inplace= True)
    data = data.drop(labels=0, axis=0)
    return data


def change_dtypes(data):
    
    data = data[['Horário',    'Position',       'Ativo',        'Tipo',
            'Volume',    'Preço In',       'S / L',       'T / P',
       'Horário Out',   'Preço Out', 'Lucro']]
    
    # change dtypes
    data['Horário'] = pd.to_datetime(data['Horário'], format= '%Y-%m-%d ')
    data['Horário Out'] = pd.to_datetime(data['Horário Out'], format= '%Y-%m-%d ')

    data['Position'] = data['Position'].astype( np.int64)
    data['Preço In'] = data['Preço In'].astype( float )
    data['Preço Out'] = data['Preço Out'].astype( float )

    data['Lucro'] = data['Lucro'].astype( np.int64)
    
    return data

def feature_extraction(df1):
    
    df1['Date'] = df1['Horário'].dt.normalize()
    df1['year'] =  df1['Horário'].dt.year
    df1['month'] = df1['Horário'].dt.month
    df1['day'] =   df1['Horário'].dt.day

    # Market classification

    df1['Market']= df1['Ativo'].apply(lambda x: 'futures' if x.startswith(('WIN', 'WDO')) else

                       'stock' if len(x) == 5 else 'options' )


    # day trade = 1 if day trade, 0 if not
    df1['day_trade']= df1.apply(lambda x: 1 if x['day'] == x['Horário Out'].day else 0, axis=1 )
    
    return df1

def financial_reports(df1):

    df1['yesterday'] = today - datetime.timedelta(days=1)

    yesterday_report = df1.loc[df1['Date'] == df1['yesterday'],['Horário',    'Position',       'Ativo',        'Tipo',
                'Volume',  'Lucro', 'Market', 'day_trade']]



    ## Save Report
    #yesterday_report.to_excel('reports/report{}.xls'.format(y))
    yesterday_report
    
    
    
    # Group by Date nr of trades and sum of Profif/loss
    profit_loss = df1[['Date', 'Lucro']].groupby('Date').sum().reset_index()

    trade_nr = df1[['Date', 'Ativo']].groupby('Date').count().reset_index()

    #merge 
    aux = pd.merge( trade_nr,profit_loss, how='inner', on='Date')
    aux.columns =['Date', 'Nr of Trades', 'Profit/Loss']

    # Last 10 days - summary (to_excel)
    summary_10_last_days = aux.sort_values('Date', ascending = False).head(10)
    summary_10_last_days.to_excel('reports/last_10_days_update{}.xlsx'.format(y))
    
    
    #Summarize stock, deal and total profit/loss per date using agg. 
    total_report = df1.groupby(['Date', 'Ativo']).agg( Deals=('Ativo','count'),Profit_Loss=('Lucro','sum')
                                            ).sort_values('Date', ascending= False)

    total_report.to_excel('reports/total_report_summary{}.xlsx'.format(y))
    
    
    return yesterday_report, total_report


def cash_flow(df1):
    
    #Filtering Variables
    daily_spend = df1.groupby('Date').agg({'Lucro':['count','sum']}).reset_index()
    daily_spend= daily_spend.rename(columns={"count": "Nr. Trades", "sum": "Profit/Loss"})
    
    #preparing DataFrame
    c_flow = pd.DataFrame()

    # start initial blance
    initial = 10000

    initial_balance=[]
    initial_balance.append(initial)


    #c_flow.values
    lucro = daily_spend['Lucro']['Profit/Loss'].values
    tax = abs(lucro*0.01)

    for i in range(daily_spend.shape[0] -1):
        initial = initial + lucro[i] - tax[i]
        initial_balance.append(initial)

    #initial_balance
    c_flow['Date'] = daily_spend['Date']
    c_flow['initial_balance'] = initial_balance
    c_flow['Lucro'] = daily_spend['Lucro']['Profit/Loss']
    c_flow['withholding_tax']= c_flow['Lucro'].apply(lambda x: abs(0.01*x) )
    c_flow['final_balance'] = c_flow['initial_balance'] + c_flow['Lucro']- c_flow['withholding_tax']

    c_flow.sort_values('Date', ascending = False)

    # Export 
    c_flow.sort_values('Date', ascending = False).to_excel('reports/cashflow{}.xlsx'.format(y))
    
    return None

def income_tax(data):
        ### Tax Rules
    # each trade = 1% withholding tax if profit > 0 
    # income tax is calculated by month = 20% * profit - withholdtax if it´s a day trade, normal trade 15%, if loss no tax

    data['withholding_tax'] = data['Lucro'].apply(lambda x: abs(0.01*x) if x > 0 else 0 )

    data['Profit_minus_withholding_tax'] = data.apply (lambda x: x['Lucro'] - x['withholding_tax'] if x['Lucro'] > 0 else x['Lucro'], axis =1 )



    aux_tax_0 =  data.groupby(['year','month', 'Market', 'day_trade']).agg( Profit= ('Lucro', 'sum'),Withholding_tax=('withholding_tax','sum'),Deals=('Ativo','count'),Profit_minus_tax= ('Profit_minus_withholding_tax', 'sum')).reset_index()


    aux_tax_0['income_tax'] = aux_tax_0.apply(lambda x: 0.20 * x['Profit_minus_tax'] if (x['day_trade'] == 1) & (x['Profit_minus_tax'] > 0) else 0.15* x['Profit_minus_tax'] if   (x['day_trade'] == 0 )& (x['Profit_minus_tax'] > 0)   else 0, axis=1 )
    aux_tax_0['Profit_net'] = aux_tax_0.apply(lambda x: x['Profit_minus_tax'] - x['income_tax'], axis=1 )


    aux_tx_summary = aux_tax_0.groupby(['year','month', 'day_trade']).agg(Deals=('Deals','count'),Profit= ('Profit', 'sum'),Withholding_tax=('Withholding_tax','sum'), Income_tax = ('income_tax', 'sum'), Profit_net = ('Profit_net', 'sum') ).sort_values('month')

    aux_tx_by_mkt = aux_tax_0.groupby(['year','month', 'Market','day_trade']).agg(Deals=('Deals','count'),Profit= ('Profit', 'sum'),Withholding_tax=('Withholding_tax','sum'), Income_tax = ('income_tax', 'sum'), Profit_net = ('Profit_net', 'sum') ).sort_values('month')
    aux_tx_summary.to_excel('reports/tx_per_month-updated{}.xlsx'.format(y))
    aux_tx_by_mkt.to_excel('reports/tx_per_mkt-updated{}.xlsx'.format(y))

    
    return None




# capture email and password from sender
def capture(arquivo): 
    nome_arq = arquivo
    try:
        arq = open(nome_arq, 'r') # abre arquivo em modo de leitura
        conteudo = arq.readlines() # lê as linhas do aquivo e guarda em uma lista
        arq.close()

        for linha in conteudo:
            if "email" in linha:
                lista_str = linha.split('=')
                email   = lista_str[1].strip()
                

            if "senha" in linha:
                lista_str = linha.split('=')
                senha     = lista_str[1].strip()
                              
    
    except FileNotFoundError:
        print(f"Arquivo {nome_arq} não encontrado!")
        
    return email, senha



# Add another file to report , check if it´s .xls or xlsx
def files(arq, name ): 
    
    add_file= str(input('Add another file to {}´s report ( only excel, please!)? ( N to exit )'.format(name)))

    regex = '(.xls|.xlsx)$' 

    #divide by letter 
    spair_letter = [l for l in add_file   ] 
    
    # if smaller than 4, probably a exit answer
    if len(spair_letter) < 4:
            add_file = add_file.upper()[0]
            
    else:
        if add_file != "N":

            try: 
                if bool(re.search( regex, add_file )) == True  and add_file not in arq: 
                    #if excel, add file
                    arq.append(add_file)
                else: 
                    print('Please check the file path and name. ej:("./reports/file.xlsx") ')
                    
                    
            except Exception as e:
                print('Error! : ',e)        
    return arq


def send_mail_with_excel( from_email_file, name, recipient_email, subject, files_xls):
    #capture email and password "FROM"
    email, senha = capture(from_email_file)
    
    msg = EmailMessage()
    msg['Subject'] = subject + y
    msg['From'] = email
    msg['To'] = recipient_email
    
    email_content = f'''

        <p>Dear {name}, </p>
        <p>Please find attached detailed cash flow and trade report.  </p>
        <br />
        <p> <small><strong> Yesterday´s trade´s information   </strong></small> </p>
            {yesterday_report.to_html()}
        <br />
        <p>Taiz Alves</p>
        <p><small><strong>Data analyst </strong></small></p> '''

    msg.add_header('Content-Type', 'text/html')
    msg.set_payload(email_content)
    
    
    ## add more than 1 file and ask 
    
    for excel_file in files_xls:
        with open(excel_file, 'rb') as f:
            file_data = f.read()
            msg.add_attachment(file_data, maintype="application", subtype="xlsx", filename=excel_file)
    
    
    try: 
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(email, senha)
            smtp.sendmail(msg['From'], [msg['To']],msg.as_string().encode('utf-8'))
            smtp.quit()
            print('Email sent to {}'.format(recipient_email))
    except Exception as e:
        print('Error! : ',e)

    return None


if __name__ == '__main__' : 

    # get data
    data = get_data('ReportHistory.xlsx')

    #dtypes
    print(data.dtypes)


    ##transformation
    data = change_dtypes(data)

    #data dimention
    print( 'Number of Rows: {}'.format( data.shape[0] ) )
    print( 'Number of Cols: {}'.format( data.shape[1] ) )


    #transformation
    data = feature_extraction(data)
    print(data.head() )
    

    #check na after transformation
    print(data.isna().sum())


    #global variable total and yesterday(y) used in reports
    today = data['Date'].max()
    y =today - datetime.timedelta(days=1)
    y = y.strftime('%Y_%m_%d')

    #reports ( in excel and as a variable)
    yesterday_report, total_report= financial_reports(data)

    #cash flow
    cash_flow(data)

    income_tax(data)


    ## get receiver´s emails and nome 
    emails = pd.read_excel("email.xls")
    names = [ n for n in emails.Name]
    emails = [ e for e in emails.email]

    # send email
    for i in range (len(emails)): 
        arquivo = ['./reports/cashflow2021_03_28.xlsx' ,'./reports/tx_per_month-updated2021_03_28.xlsx']

        send_mail_with_excel('email_senha.txt' , names[i], emails[i],  'financial report', files(arquivo, names[i]) )
