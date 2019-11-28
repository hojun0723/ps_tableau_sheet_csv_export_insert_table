#=======================================================================
#
# 본부 현황판 내용 추출
# - urllist 파일의 통합문서들에 대해서 데이터를 추출한 후 csv 파일 생성
# - 생성된 csv 파일을 반복하여 데이터베이스 적재
#
#=======================================================================

ECHO OFF
D:
cd "D:\Tableau Server\packages\bin.20183.19.0212.1312"
"***** Tableau Server Login"
tabcmd login -s http://localhost:8000 -u administrator -p password

# CSV 통합문서의 기준월로 설정
$P_yyyyMMdd = Get-Date (Get-Date).AddMonths(-1) -format "yyyyMMdd"
$P_yyyy = $P_yyyyMMdd.Substring(0,4)
$P_MM = $P_yyyyMMdd.Substring(4,2)

Get-Date -format "yyyyMMdd"

####################################### 파일경로
$Path = "D:\dw\bi\"

####################################### 파일명
$File = "BI_TOSBI014"

####################################### URL
$Url = Get-Content D:\Tableau_Sheet_ETL_Program\002_daily_prod\url_list.txt


"********************* Extract Processing"
Get-Date -format "yyyy-MM-dd HH:mm:ss"
# URL 별로 tabcmd 실행. csv 파일 생성
For($i=0; $i -lt $Url.Length; $i++) {
	#$Url.Item($i)

    $yyyyMMdd = Get-Date -format "yyyyMMdd"
    $yyyy = $yyyyMMdd.Substring(0,4)
    $MM = $yyyyMMdd.Substring(4,2)
    $HHmmss = Get-Date -format "HHmmss"
	
	# 명령어 생성 (파일경로 + 파일명)
    $seq = $i + 1
	$expresion = $Path + $File + "_" + $yyyyMMdd + "_" + $seq + ".CSV"
	#$expresion	

	#Start-Process tabcmd export $Url.Item($i) --csv -f $expresion - Wait
	tabcmd export $Url.Item($i) --csv -f $expresion
}

"********************* Extract Processing Complete"
Get-Date -format "yyyy-MM-dd HH:mm:ss"

#exit

# Path 폴더의 파일 리스트를 배열로. 당일 생성된 파일만 대상으로 함
$SearchFile = $File + "_" + $yyyyMMdd + "*"
$FileNames = Get-ChildItem -Path $Path -Name $SearchFile -File

#$FileNames.Item(0)
#$FileNames
#$FileNames.Length

####################################### db 접속
$DBServer = "123.123.123.123"    #"(localdb)\v11.0"
$DBName = "database"
$uid = "id"
$pwd = "password"
$sqlConnection = New-Object System.Data.SqlClient.SqlConnection
$sqlConnection.ConnectionString = "Server=$DBServer;Database=$DBName;Integrated Security=True;User ID = $uid; Password = $pwd;"     #"Server=$DBServer;Database=$DBName;Integrated Security=True;"
$sqlConnection.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.connection = $sqlConnection

####################################### 적재 테이블명
$table = 'TOSBI014'


"********************* Load Processing"
Get-Date -format "yyyy-MM-dd HH:mm:ss"
# 파일별로 sql 테이블 INSERT
$sql = ""
For($i=0; $i -lt $FileNames.Length; $i++) {

    $CRT_USER_ID = 'ISP'
    $DATA_CRT_DTM = Get-Date -UFormat "%Y-%m-%d %T"

    $csv_path = $Path + $FileNames.Item($i)

    $csv = Get-Content $csv_path -Encoding UTF8

    ####################################### 헤더가 있는 csv 인 경우 $j = 1, 없는 경우 0 으로 설정
    For($j=1; $j -lt $csv.Length; $j++) {
        
        $csv_arr = $csv.Item($j).Split(',', 5)

        $col1 = $csv_arr.Item(0)
        $col2 = $csv_arr.Item(1)
        $col3 = $csv_arr.Item(2)
        $col4 = $csv_arr.Item(3)
        $col5 = $csv_arr.Item(4)
        $col5 = $col5.Replace(",", "")
        $col5 = $col5.Replace("""", "")

        $sql = $sql + "INSERT INTO "
        $sql = $sql +  $table
        $sql = $sql + " VALUES ('$col1', '$col2', '$col3', '$col4', '$col5', '$CRT_USER_ID', '$DATA_CRT_DTM');"
    }

}

#$sql

$cmd.commandtext = $sql
$cmd.executenonquery()
$sqlConnection.Close()

"********************* Load Processing Complete"
Get-Date -format "yyyy-MM-dd HH:mm:ss"

# url 리스트 파일 읽어서 for 문으로 csv 파일 생성. 
# 해당일자 해당파일명인 파일만 리스트로 만들어서 테이블 적재 구현