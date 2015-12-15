<?php
/**
 * A class BDados estabelece a ligação e executa todas as instruções na Base de dados. Compatível com MYSQL, ORACLE, SQLITE, PGSQL, etc
 * @author dpereira
 * @package MOON
 * @name BDados
 * @version 1.0
 */
class BDados {

	public $linkID;
	public $linkSqli;
	protected $queryID;
	protected $errNo;
	protected $errStr;
	private $registosAfectados;
	private $lastID;
	protected $nomeBD;
	private $sql;
	private $vstmt;
	public $debug;
	protected $BD;

	/**
	 * construtor
	 * @name __construct
	 * @param Servidor
	 * @param Utilizador BD
	 * @param Password do utilizador BD
	 * @param Base de dados
	 * @param Driver PDO
	 * @param Depuração
	 */
	function __construct($hostname, $username, $password, $database, $driver, $debug=false) {

		$this->debug = $debug;
		$this->BD = $driver;
		$this->nomeBD = $database;
		$this->linkID = 0;
    	$this->linkSqli = 0;
		$this->queryID = 0;
		$this->errNo = 0;
		$this->errStr = '';
		$this->registosAfectados = 0;
		$this->lastID = -1;
		$this->sql = "";
		$this->vstmt = null;

		if (!($hostname and $username and $database)) {
			$this->halt('Faltam parametros na ligação.');
		}

		try {
			$this->linkID = new PDO($driver.':host='.$hostname.';dbname='.$database.';charset=utf8', $username, $password);
			$this->linkID->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		} catch (MoonException $e) {
			$this->halt( "Erro de Conexão " . $e->getMessage() . "\n");
			exit;
		}
	}

	/**
	 * Operações a executar quando o objecto é destruído
	 * @name __destruct
	 */
	function __destruct()
	{
		$this->fechaRs();
		$this->fecha();
	}

	/**
	 * Envia erro detectado e cria excepção
	 * @name halt
	 * @param mensagem
	 */
	private function halt($wMsg){
		$this->enviaErro($wMsg);
		#trigger_error(sprintf("Erro na BD %s: %s \n erro n. %s \n descrição: %s \n registos afectados: %s", $this->nomeBD, $wMsg, $this->errNo, $this->errStr, $this->registosAfectados), E_USER_ERROR);
	}

	/**
	 * Envia erro por mail com dados e variáveis de ambiente
	 * @name enviaErro
	 * @param Numero excepção
	 */
	protected function enviaErro($MoonException=0) {
		/* ainda n está como deveria... */
		include_once($_SERVER['DOCUMENT_ROOT'] . "/inc/base.inc.php");
		$v_msg = ($MoonException) ? $MoonException->getMessage() : "";
		$v_de = "dev+fiat@moonlight.pt";
		$v_para = "dev@moonlight.pt";
		$v_assuntoEmail = "::DEV:: ERRO " . $_SESSION["CONF"]["nome_aplicacao"];
		$v_erroDB = sprintf("Erro na BD %s: %s \n erro n. %s \n descrição: %s \n registos afectados: %s", $this->nomeBD, $wMsg, $this->errNo, $this->errStr, $this->registosAfectados);
		$v_corpoEmail = "<h2>Mensagem de erro: " . $v_msg . " </h2>
                    <hr/>
                    <pre>
                    <br/>Pag:" . __FILE__ . "
                    <br/>Class:" . __CLASS__ . "
                    <br/>Função:" . __FUNCTION__ . "
                    <br/>Método:" . __METHOD__ . "
                    <br/>BD: " . $v_erroDB . "
                    <br/>SQL: " . $this->sql . "
                    </pre>
                    <hr/><b>GET</b><hr/><pre>" . Utils::dump($_GET) . "</pre>
                    <hr/><b>POST</b><hr/><pre>" . Utils::dump($_POST) . "</pre>
                    <hr/><b>COOKIES</b><hr/><pre>" . Utils::dump($_COOKIES) . "</pre>
                    <hr/><b>SESSION</b><hr/><pre>" . Utils::dump($_SESSION) . "</pre>
                    <hr/><b>CLASS</b><hr/><pre>" . Utils::dump(get_class_vars(get_class($this))) . "</pre>
                    <hr/><b>Outros</b><hr/><pre>" . serialize($this) . "</pre>
                    ";
		Utils::enviamail($v_de, $v_para, "", "", $v_assuntoEmail, $v_corpoEmail, "nenhum");
	}

	/**
	 * Executa uma string SQL
	 * @name executaMisto
	 * @param SQL
	 * @param Se for para fazer 'commit' à instrução
	 */
	function executaMisto($sql, $commit=true) {

		//o argumento commit permite que esta funcao seja usada para vários inserts/updates fazendo o commit no final
		$this->sql = trim($sql);
		//echo $sql;
		if ($this->debug)
			printf("SQL query: %s<br />\n", $this->sql);
		try
		{
			try {
				$this->vstmt = $this->linkID->prepare($this->sql);
				try {
					$this->vstmt->execute();
					$this->lastID = $this->linkID->lastInsertId();
					$this->registosAfectados += $this->vstmt->rowCount();
					$this->queryID = $this->vstmt;
				} catch(PDOExecption $e) {
					$this->errNo	= $this->linkID->errorCode();
					$this->errStr 	= $this->linkID->errorInfo();
					$this->halt( $e->getMessage());
					$this->registosAfectados = -1;
				}
			} catch( PDOExecption $ex ) {
				$this->errStr = $ex->getMessage();
				$this->halt( $ex->getMessage());
				$this->registosAfectados = -1;
			}

			if ($commit) $this->commit();

		} catch (MoonException $e) {
			$this->enviaErro($e);
			exit();
		}
		return $this->queryID;
	}

	/*function executaMistoStatment($stmt, $commit=true) {

		//o argumento commit permite que esta funcao seja usada para vários inserts/updates fazendo o commit no final
		if ($this->debug)
			printf("PDO Statement: %s<br />\n", $stmt);
		try
		{
			try {
				$this->vstmt->execute();
				$this->lastID = $this->linkID->lastInsertId();
				$this->registosAfectados += $this->vstmt->rowCount();
				$this->queryID = $this->vstmt;
			} catch(PDOExecption $e) {
				$this->errNo	= $this->linkID->errorCode();
				$this->errStr 	= $this->linkID->errorInfo();
				$this->halt( $e->getMessage());
				$this->registosAfectados = -1;
			}
			if ($commit) $this->commit();

		} catch (MoonException $e) {
			$this->enviaErro($e);
			exit();
		}

		return $this->queryID;
	}*/

    function executaMysqlProc($proc,$_dados,$accao)
    {
        try
        {
            $dadosAretornar = "";
            $strAct = "";

            if(strtoupper($accao) == "INS")
                $strAct = "insert";

            else if(strtoupper($accao) == "UPD")
                $strAct = "update";

            else if(strtoupper($accao) == "DEL")
                $strAct = "delete";

				//$query = "select convert(param_list using latin1) campos from mysql.proc where db='".$_SESSION["CONF"]["database"]."' and name='".$strAct."_".$proc."'";
				$query = "SELECT PARAMETER_NAME FROM information_schema.parameters WHERE SPECIFIC_NAME='".$strAct."_".$proc."' AND PARAMETER_MODE='OUT' LIMIT 1";

				$res = $this->executaMisto($query);
				$temOut = $res->fetchColumn();

				$res = ")";
			 	if($temOut!=""){
					$res = ", @res); select @res;";
				}

				$testLength = count($_dados);

				$parametersHolder = str_repeat(",?", $testLength-1);
				$stmt = $this->linkID->prepare("CALL ".$strAct."_".$proc."(?".$parametersHolder.$res);

				$stmt->execute($_dados);

				if($temOut!=""){
					$stmt->nextRowset();
					$dadosAretornar = $stmt->fetchColumn();
				}
				$stmt = null;

				return $dadosAretornar;
        }
        catch(MoonException $e)
        {
            $this->enviaErro($e);
            $this->linkID = null;
				exit();
        }
    }

	/**
	 * Executa uma procedure - MSSQLServer
	 * @name executaMSsqlProc
	 * @param proc = procedure a correr no MSSQLServer
	 * @param _dados = Valores enviados
	 * @param accao = operaçãoa a desenvolver (INS/UPD/DEL)
	 * @param wDebug= depuração. Omissão: false
	 */
	function executaMSsqlProc($proc,$_dados,$accao,$separador=',')
    {
        try
        {
            $strAct = "";

            if(strtoupper($accao) == "INS")
            {
                $strAct = "insert";
                /*
				$proc_to_call = "CALL ".$strAct."_".$proc."(".$dados.")";
				$res = $this->executaMisto($proc_to_call);
				return $res;
				*/

            }
            else if(strtoupper($accao) == "UPD")
            {
                $strAct = "update";
                /*
                $proc_to_call = "CALL ".$strAct."_".$proc."(".$dados.");";
                $this->executaMisto($proc_to_call);
                return "ok";
                */
            }
            else if(strtoupper($accao) == "DEL")
            {
                $strAct = "delete";
                /*
                $proc_to_call = "CALL ".$strAct."_".$proc."(".$dados.")";
                $this->executaMisto($proc_to_call);
                return "ok";
                */
            }

            $query = "select convert(param_list using latin1) campos from mysql.proc where db='".$_SESSION["CONF"]["database"]."' and name='".$strAct."_".$proc."'";
			/*$query = " SELECT  P.parameter_id AS [ParameterID],
							P.name AS [ParameterName],
							TYPE_NAME(P.user_type_id) AS [ParameterDataType],
							P.max_length AS [ParameterMaxBytes],
							case when P.is_output =1
								 then 'OUT'
								else 'IN'
							end
							AS [IsOutPutParameter]

						FROM sys.objects AS SO
						INNER JOIN sys.parameters AS P ON SO.OBJECT_ID = P.OBJECT_ID
						WHERE SO.OBJECT_ID IN ( SELECT OBJECT_ID
								FROM sys.objects
								WHERE TYPE IN ('P','FN'))
						and SO.name ='moon_".$strAct."_".$proc."'
						ORDER BY SO.name, P.parameter_id";*/


			//echo $query;

			$Exec = $this->linkID->prepare($query);
			$Exec->execute();
			$dadosLinha = array();
			$resExec = $Exec->fetchAll(PDO::FETCH_ASSOC);
			$numTotalParams = count($resExec);
			## print_r($resExec); exit();

			#for($i=0; $row = $Exec->fetchAll(PDO::FETCH_ASSOC); $i++){
			for($i=0; $i<$numTotalParams; $i++){
				$row = $resExec[$i];
				if($row['IsOutPutParameter']=='IN'){
					$params .= $row['ParameterName'].'=:w'.str_replace('@', '', $row['ParameterName']).',';
					#$params .= ''.$row['ParameterName'].'=:'.$row['ParameterName'].',';
				}else{
					#$params .= $row['ParameterName'].",";
					$params .= $row['ParameterName'].'=:w'.str_replace('@', '', $row['ParameterName']).',';
				}
				$dadosLinha[] = $row;
			}
			$params = substr($params,0,-1);

         $valores = explode($separador, $_dados);

			#$proc_to_call = "CALL moon_".$strAct."_".strtolower($proc)."(".$params.")";
			#$proc_to_call = "EXEC moon_".$strAct."_".strtolower($proc)." ".$params;
			$proc_to_call = "{:retval = CALL moon_".$strAct."_".strtolower($proc)." (".$params.")}";
			#echo '??'.$proc_to_call;exit();

			$this->vstmt = $this->linkID->prepare($proc_to_call);

			$retval = null;
			for($i=0; $i<count($dadosLinha); ++$i)
      	{
				## para o 1º elemento retval
				if($i==0){
					$this->vstmt->bindParam('retval', $retval, PDO::PARAM_INT|PDO::PARAM_INPUT_OUTPUT, 50);
				}

				$vParameterName = "w".str_replace('@', '', $dadosLinha[$i]["ParameterName"])."";
				#$vParameterName = "'".$dadosLinha[$i]["ParameterName"]."'";
				//$vParameterName = $dadosLinha[$i]["ParameterID"];

				if ($dadosLinha[$i]["ParameterDataType"]=="int"){
					$vParamType = PDO::PARAM_INT;
					$vMaxBytes = 50;
				}else{
					$vParamType = PDO::PARAM_STR;
					if($dadosLinha[$i]["ParameterMaxBytes"]==-1){
						$this->vstmt->setAttribute(PDO::SQLSRV_ATTR_ENCODING, PDO::SQLSRV_ENCODING_SYSTEM);
					}
					$vMaxBytes = $dadosLinha[$i]["ParameterMaxBytes"];
				}

				if($dadosLinha[$i]["IsOutPutParameter"]=="IN")
				{
					#Param IN
					#$this->vstmt->bindValue($vParameterName, $valores[$i], $vParamType);
					$this->vstmt->bindParam($vParameterName, $valores[$i]);
                }
				else
				{
					#Param OUT
					$pOut='';
					$vParamType .= "|".PDO::PARAM_INPUT_OUTPUT;
					#$this->vstmt->bindValue($vParameterName, $ParamOut, $vParamType, $dadosLinha[$i]["ParameterMaxBytes"]);
					#$this->vstmt->bindParam($vParameterName, $pOut, $vParamType, $dadosLinha[$i]["ParameterMaxBytes"]);
					$this->vstmt->bindParam($vParameterName, $pOut, $vParamType, $vMaxBytes);
				}
         }

			$this->vstmt->execute();
            #print_r($this->vstmt);
			#echo '$pOut: '.$pOut.'#';

			$dadosAretornar = $this->vstmt;

			if($pOut!=''){
				return $pOut;
			}
			else {
				return $dadosAretornar;
			}

         $this->linkID = null;

        }
        catch(MoonException $e)
        {
            $this->enviaErro($e);
            $this->linkID = null;
				exit();
        }
    }

	/**
	 * Retorna todas as linhas de um SQL
	 * @name dameLinhas
	 * @param SQL
	 * @param TRUE se vierem devidamente identificadas com os nomes das colunas do SQL
	 */
	function dameLinhas($sql, $assoc=true, $ini=0, $fim=999999999) {
		if (empty($sql)) $this->halt('Falta o SQL do dameLinhas');
		$rows = array();

		$result = $this->executaMisto($sql);
		if ($assoc)
			$rows[] = $result->fetchAll(PDO::FETCH_ASSOC);
		else
			$rows[] = $result->fetchAll(PDO::FETCH_NUM);

		$rows = $rows[0];

		#limpezas
		#oci_free_statement( $this->queryID );
		#$this->fechaRs();

		return $rows;
	}

	/**
	 * Retorna um Cursor duma procedure Oracle
	 * @name getCursor
	 * @param $wProc =
	 * @example getCursor("dec_rtn_recordset(201101, :cur);");
	 */
	function getCursor($wProc, $assoc=true){
	}

	/**
	 * Retorna apenas a primeira linha de um SQL
	 * @name damePrimeiraLinha
	 * @param SQL
	 * @param TRUE se vierem devidamente identificadas com os nomes das colunas do SQL
	 */
	function damePrimeiraLinha($sql, $assoc=true) {
		$rows = array();
		/*
		echo $sql."<br>";
		echo "BD=".$this->BD;exit();
		*/
			$result = $this->executaMisto($sql);
			if($result===false){
			}else{
				if ($assoc)
					$rows[] = $result->fetch(PDO::FETCH_ASSOC);
				else
					$rows[] = $result->fetch(PDO::FETCH_NUM);
			}
		return $rows[0];
	}

	/**
	 * Retorna o numero de campos do SQL
	 * @name dameNumColunas
	 */
	function dameNumColunas() {
	}

	/**
	 * Retorna o numero de Linhas retornados
	 * @name dameNumLinhas
	 */
	function dameNumLinhas() {
			return $this->vstmt->rowCount();
	}

	/**
	 * Retorna o numero de registos afectados anteriormente
	 * @name dameRegistosAfectados
	 */
	function dameRegistosAfectados() {
		return $this->registosAfectados;
	}

	/**
	 * Retorna o ultimo Id inserido ou o valor duma sequencia do oracle
	 * @name dameUltimoId
	 */
	function dameUltimoId($wSeq=null) {
			return $this->lastID;
	}

	/**
	 * Define o início duma transacção
	 * @name begin
	 */
	function begin() {

	}

	/**
	 * Faz rollback a transacção que não correu como esperado
	 * @name rollback
	 */
	function rollback() {

	}

	/**
	 * Faz commit a uma transacção
	 * @name commit
	 */
	function commit() {

	}

	/**
	 * Fecha Resource
	 * @name fechaRs
	 */
	function fechaRs() {
			$this->queryID = null;
	}

	/**
	 * Fecha Ligação à BD
	 * @name fecha
	 */
	function fecha() {
			$this->linkID = null;
	}
}

?>
