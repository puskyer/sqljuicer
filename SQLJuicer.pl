#!/usr/bin/perl -w
# Lista as transacoes de um banco pelo transaction log
# uso: SQLJuicer.pl -s servidor -b banco_de_dados [-h]
# 
# -b :nome do banco
# -s :nome do servidor
# -h :mensagem de ajuda
# 
# Dependecias:
#       SQLCMD
#
#
use Getopt::Std;
use Time::Local;
use Date::Calc qw(Add_Delta_Days);
use Win32::OLE;
use Win32::OLE::Const 'Microsoft ActiveX Data Objects';

my $ver="0.1";

#opcoes
%args = ( );
getopts("b:s:h", \%args);

#coloca mensagem explicativa
if ($args{h}) {
   &cabecalho;
   print <<DETALHE ;
uso: SQLJuicer.pl -s servidor -b banco_de_dados [-h]
 
 -b :nome do banco
 -s :nome do servidor
 -h :mensagem de ajuda

DETALHE
   exit;
}

die "Entre com o nome do servidor SQL Server\n" unless ($args{s});

die "Entre com o nome do banco de dados SQL Server\n" unless ($args{b});

# ----- variaveis do script
my @tranDet;
my %dados;
my $banco = $args{b};
my $servidor = $args{s};

my %ultlinha = ( );


#--------------------------

&cabecalho;

#pega cada transaction ID onde houve commit
&captatranID(\@tranDet, $banco, $servidor);

#Varre cada transacao na ordem inversa que ocorreram
foreach my $pID (@tranDet) {

   #pega a data da transacao
   my $dtTr = $pID->{DATA};

   if ($dtTr) {

      #Traduz as operacoes
      &captalinhasOper($pID, $banco, $servidor, \%ultlinha);

   }
}

#lista os resultados

foreach my $pID (reverse @tranDet) {

   print "=" x 75 ."\n";
   print "  Data: $pID->{DATA}     Transacao: $pID->{TID}\n";

   foreach my $aDados (@{$pID->{DADOS}}) {
      print "Tabela: $aDados->{TAB}       Tp Oper: $aDados->{TIPO}\n";
      print "Chaves: \n";

      foreach my $chave (keys %{$aDados->{CHAVES}}) {print " " x 9 . "$aDados->{CHAVES}->{$chave}\n";}

      print "Valores: \n";
      foreach my $modif (@{$aDados->{MODIF}}) {print " " x 9 . "$modif\n";}
      print "\n";
   }
}

#### FIM DO PROGRAMA PRINCIPAL #####

####################################### Sub rotinas  ######################################################

sub cabecalho {
   print <<CABEC;

SQLJuicer.pl v$ver
Lista Transacoes do Transaction Log
http://code.google.com/p/sqljuicer/
--------------------------------------------------------------------------

CABEC
}



#-----------------------------------------------------------------------------------------------------

sub captatranID {

  my $p1 = shift;
  my $banco = shift;
  my $server = shift;


  my $Conn = Win32::OLE->new("ADODB.Connection");
  my $RS   = Win32::OLE->new("ADODB.Recordset"); 

  my $DSN = "Provider=SQLNCLI;Server=$server;Database=$banco;Trusted_Connection=yes;";


  $Conn->Open($DSN);


  #busca os transaction ID de todas as operacoes com COMMIT, em ordem decrescente de datas
  my $SQL ="Select [Transaction ID], [End Time] from ::fn_dblog(null,null) where Operation = \'LOP_COMMIT_XACT\' Order by [End Time] DESC";

  $RS->Open($SQL, $Conn, 1, 1);

  until ($RS->EOF) {

     my $rec = {
        TID  => $RS->Fields("Transaction ID")->value, 
        DATA => $RS->Fields("End Time")->value, 
     };

     $p1->[scalar(@$p1)] = $rec;
   
     $RS->MoveNext;
  }

  $RS->Close;
  $Conn->Close;

}

#---------------------------------- UTILITARIOS ------------------------------------------------------

sub execquery {
#executa queries usando o sqlcmd

  my $cmd = shift;
  my $server = shift;

  my $comstr = "sqlcmd -S $server -E -Q \"$cmd\"";

  my $resp = `$comstr`;

  return $resp;
}

#-----------------------------------------------------------------------------------------------------

sub captabytes {
#busca a sequencia de bytes relativa ao pageID e ao Slot dentro do resultado do dbcc page

   my $ent = shift;
   my $slotID = shift;

   my ($linhas)= ($ent =~ /Slot $slotID Offset 0x[0-9A-Fa-f]* .*Memory Dump \@0x[0-9A-Fa-f]*\s*\n\n(.+)\nSlot $slotID Column 0/s);

   my $ret = "";
   while ($linhas =~ /[0-9A-Fa-f]+\:\s+([0-9A-Fa-f]+)\s*([0-9A-Fa-f]*)\s*([0-9A-Fa-f]*)\s*([0-9A-Fa-f]*)/g) {
      $ret .= "$1$2$3$4"; 
   }

   return $ret
}

#-----------------------------------------------------------------------------------------------------

sub stringtoarray {
# transforma uma string de bytes extraida do resultado do dbcc page em um array de bytes

   my $bytes = shift;

   my @byteslinha=();
   while ($bytes =~ /([0-9A-Fa-f]{2})/g) {$byteslinha[scalar(@byteslinha)]=hex($1);};   

   return @byteslinha;
}

#-----------------------------------------------------------------------------------------------------

sub bytetoword {
# transforma um array de bytes em um array de words (unsigned small int)      

   my (@wordaux) = @_;  
   
   #adiciona um elemento 0 no final se tem numero impar de bytes
   $wordaux[scalar(@wordaux)] = 0 if (scalar(@wordaux) % 2 == 1);

   #separa em unsigned integer, invertendo o little endian
   my @wordslinha=();

   for (my $i = 1; $i <= (scalar(@wordaux)/2); $i++) {$wordslinha[scalar(@wordslinha)]=($wordaux[2*$i-1] << 8) + $wordaux[2*$i-2];}                

   return @wordslinha;
}

#-----------------------------------------------------------------------------------------------------

sub diferente {
# retorna true se os parametros tem valores diferentes

   my ($a, $b) = (@_);

   if ($a =~ /^\d+\.?\d+$/) {return ($a!=$b);}
   else {return ($a ne $b)}

}

#-----------------------------------------------------------------------------------------------------

sub captatab {
# busca o ObjectID da tabela usada na alteracao

   my $ent = shift;
   my $banco = shift;
   my $server = shift;

   my ($tabID) = ($ent =~ /Metadata\: ObjectId \=\s+(\d+)/s);

   return ($tabID, &captabID($tabID, $banco, $server));

}

#----------------------------------- UTILITARIOS DE BANCO -----------------------------------------------------------------

sub captachaves {
# mapeia os campos chaves da linha alterada

   my $tabID = shift;
   my $banco = shift;
   my $server = shift;
   my $refchave = shift;
   my $slotID = shift;
   my $pagina = shift;

   my $Conn = Win32::OLE->new("ADODB.Connection");
   my $RS   = Win32::OLE->new("ADODB.Recordset"); 

   my $DSN = "Provider=SQLNCLI;Server=$server;Database=$banco;Trusted_Connection=yes;";


   $Conn->Open($DSN);


   my $SQL ="SELECT sc.name as coluna FROM $banco.sys.indexes AS i, $banco.sys.index_columns AS ic, syscolumns as sc" .
            " WHERE (i.is_primary_key = 1) and (i.object_id = $tabID) and (i.OBJECT_ID = ic.OBJECT_ID) and " .
            "(sc.id = ic.OBJECT_ID) AND (i.index_id = ic.index_id) AND (ic.column_id = sc.colid)";

   $RS->Open($SQL, $Conn, 1, 1);

   until ($RS->EOF) {
      $refchave->{$RS->Fields("coluna")->value}="";        
         
      $RS->MoveNext;
   }

   $RS->Close;
   $Conn->Close;

   #faz um parse dos valores da pagina, buscando a coluna especifica
   #foreach $i (keys %$refchave) {

#       if ($pagina =~ /Slot $slotID Column $i Offset 0x[0-9A-Fa-f]+ Length \d+\s*\n\n(\w+\s*\=.+)/) {
#          $refchave->{$i} = $1;
#       } 
#   } 

}

#-----------------------------------------------------------------------------------------------------

sub captaestruturatabela {
#mapeia a estrutura da tabela

   my $tabID = shift;
   my $banco = shift;
   my $server = shift;
   my $ptable = shift;

   my $Conn = Win32::OLE->new("ADODB.Connection");
   my $RS   = Win32::OLE->new("ADODB.Recordset"); 

   my $DSN = "Provider=SQLNCLI;Server=$server;Database=$banco;Trusted_Connection=yes;";


   $Conn->Open($DSN);


   my $SQL ="SELECT sc.colorder, sc.name, sc.xusertype, sc.length, st.variable, sc.xscale FROM syscolumns sc, " .
               "systypes st WHERE sc.xusertype = st.xusertype and sc.id = $tabID " .
               "ORDER BY st.variable, colorder";

   $RS->Open($SQL, $Conn, 1, 1);

   #a primeira coluna fixa comeca apos 4 bytes de controle
   my $offset = 4;

   until ($RS->EOF) {
      my $coluna = $RS->Fields("colorder")->value;
      my $nome = $RS->Fields("name")->value;
      my $tipo = $RS->Fields("xusertype")->value;
      my $tam = $RS->Fields("length")->value;
      my $varia = $RS->Fields("variable")->value;
      my $escala = $RS->Fields("xscale")->value; 
         
      my $rec = {
         COLUNA => $coluna, 
         NOME   => $nome,
         TIPO   => $tipo,
         TAM    => $tam,
         DESLOC => ($varia == 0 ? $offset : 0),
         ESCALA => $escala,
         VARIA  => $varia
      };

      $ptable->[scalar(@$ptable)] = $rec;

      $offset += $tam;

      $RS->MoveNext;
   }

   $RS->Close;
   $Conn->Close;

}

#-----------------------------------------------------------------------------------------------------

sub captabID {
#pega o nome da tabela pelo ObjectID

   my $tabID = shift;
   my $banco = shift;
   my $server = shift;

   my $ret;

   my $Conn = Win32::OLE->new("ADODB.Connection");
   my $RS   = Win32::OLE->new("ADODB.Recordset"); 

   my $DSN = "Provider=SQLNCLI;Server=$server;Database=$banco;Trusted_Connection=yes;";


   $Conn->Open($DSN);

   my $SQL = "Select name from $banco..sysobjects where id = $tabID";

   $RS->Open($SQL, $Conn, 1, 1);

   until ($RS->EOF) {
      $ret = $RS->Fields("name")->value;
   
      $RS->MoveNext;
   }

   $RS->Close;
   $Conn->Close;


   return $ret;

}

#-----------------------------------------------------------------------------------------------------

sub decodifica_valores {
   my $tipo = shift;
   my $refvalores = shift;
   my $escala = shift;

   my $valor;

   if ($tipo == 56) {
      # int
      $valor = ($refvalores->[3] << 24) + ($refvalores->[2] << 16) + ($refvalores->[1] << 8) + $refvalores->[0];
   }
   elsif ($tipo == 48) {
      # tynint
      $valor = $refvalores->[0];
   }
   elsif ($tipo == 52) {
      # smallint
      $valor = ($refvalores->[1] << 8) + $refvalores->[0];
   }
   elsif ($tipo == 127) {
      # bigint
      $valor = ($refvalores->[7] << 56) + ($refvalores->[6] << 48) + ($refvalores->[5] << 40) + 
               ($refvalores->[4] << 32) + ($refvalores->[3] << 24) + ($refvalores->[2] << 16) + 
               ($refvalores->[1] << 8) + $refvalores->[0];
   }
   elsif (($tipo == 106) || ($tipo == 108)) {
      # numeric e decimal
      $valor = 0;

      if ($refvalores->[0] == 1) {
         #o byte 0 sempre vem com o valor 01 (?) entao esta sendo pulado
         for (my $i=1; $i < scalar(@$refvalores); $i++) {
        
             $valor += ($refvalores->[$i] << (8*($i-1)));
         }
 
         $valor = sprintf("%.${escala}f",$valor/(10**$escala));
      }
      else {
         $valor = "null";
      }
 
   }
   elsif ($tipo == 104) {
      # bit
      $valor = $refvalores->[0];
   }
   elsif ($tipo == 175) {
      # char
      $valor = pack("C*", @$refvalores);
   }
   elsif ($tipo == 239) {
      # nchar

      #formata a string unicode
      $valor = pack("U*", &bytetoword(@$refvalores));            
   }   
   elsif ($tipo == 167) {
      # varchar
      $valor = pack("C*", @$refvalores);
   }
   elsif ($tipo == 231) {
      # nvarchar
      $valor = pack("U*", &bytetoword(@$refvalores));
   }
   elsif ($tipo == 61) {
      # datetime
      my $datap = ($refvalores->[7] << 24) + ($refvalores->[6] << 16) + ($refvalores->[5] << 8) + 
                  $refvalores->[4];
            
      my ($y2, $m2, $d2) = Add_Delta_Days(1900, 1, 1, $datap);
   

      my $horap = ($refvalores->[3] << 24) + ($refvalores->[2] << 16) + ($refvalores->[1] << 8) + 
                  $refvalores->[0];
      
      my ($hh,$mm,$ss);

      {use integer;
         $horap = $horap/300;


         $hh = $horap/3600;
         $horap %= 3600;

         $mm = $horap/60;
         $ss = $horap%60; 
      }
      
      if (($datap==0) && ($horap ==0)) {
         $valor = "null";
      }
      elsif ($datap==0) {
         $valor = sprintf("%02d\:%02d\:%02d",$hh,$mm,$ss);
      }
      elsif ($horap==0) {
         $valor = sprintf("%4d\/%02d\/%02d",$y2,$m2,$d2);
      }
      else { 
         $valor = sprintf("%4d\/%02d\/%02d %02d\:%02d\:%02d",$y2,$m2,$d2,$hh,$mm,$ss);
      }   
   }
   elsif ($tipo == 58) {
      # smalldatetime
      my $datap = ($refvalores->[3] << 8) + $refvalores->[2];
            
      my ($y2, $m2, $d2) = Add_Delta_Days(1900, 1, 1, $datap);
   

      my $horap = ($refvalores->[1] << 8) + $refvalores->[0];
      
      my ($hh,$mm,$ss);

      {use integer;

         $hh = $horap/60;
         $horap %= 60;

         $mm = $horap;
         $ss = 0; 
      }
      
      if (($datap==0) && ($horap ==0)) {
         $valor = "null";
      }
      elsif ($datap==0) {
         $valor = sprintf("%02d\:%02d\:%02d",$hh,$mm,$ss);
      }
      elsif ($horap==0) {
         $valor = sprintf("%4d\/%02d\/%02d",$y2,$m2,$d2);
      }
      else { 
         $valor = sprintf("%4d\/%02d\/%02d %02d\:%02d\:%02d",$y2,$m2,$d2,$hh,$mm,$ss);
      } 
      
   }
   elsif ($tipo == 60) {
      # money
      $valor = ($refvalores->[7] << 56) + ($refvalores->[6] << 48) + ($refvalores->[5] << 40) + 
               ($refvalores->[4] << 32) + ($refvalores->[3] << 24) + ($refvalores->[2] << 16) + 
               ($refvalores->[1] << 8) + $refvalores->[0];

      $valor = sprintf("%6.2f",$valor/10000);
   }
   elsif ($tipo == 122) {
      # smallmoney
      $valor = ($refvalores->[3] << 24) + ($refvalores->[2] << 16) + 
               ($refvalores->[1] << 8) + $refvalores->[0];

      $valor = sprintf("%6.2f",$valor/10000);
   }
   elsif ($tipo == 62) {
      # float    
      $valor = "0x";
           
      #remonta a string em hexa
      foreach (@$refvalores) {$valor = $valor . sprintf("%x",$_)}
   
      $valor = unpack "d", pack "H*", $valor;   

   
   }
   else {
      # msg para os campos nao tratados
      $valor = "Tipo $tipo valor=@$refvalores"; 
   }

   return $valor;        

}

#-----------------------------------------------------------------------------------------------------

sub decodifica_colunas {
   my $refbytes = shift;
   my $refstruc = shift;
   my $coluna = shift;

   #offset do numero de colunas
   my $offnumcol = ($refbytes->[3] << 8) + $refbytes->[2];

   #numero de colunas
   my $numcol = ($refbytes->[$offnumcol+1] << 8) + $refbytes->[$offnumcol];

   my ($bytesbitmap, $valor);

   #quantidade de bytes para o null bitmap
   {use integer;
     $bytesbitmap = ($numcol/8) + (($numcol % 8) == 0 ? 0: 1);
   }

   #numero de colunas variaveis
   my $numcolvar = (scalar(@$refbytes) > ($offnumcol+3+$bytesbitmap)) ? ($refbytes->[$offnumcol+3+$bytesbitmap] << 8) + $refbytes->[$offnumcol+$bytesbitmap+2] : 0;

   #numero de colunas fixas
   my $numcolfixo = $numcol-$numcolvar;

   my $ini;

   #percorre colunas fixas
   for (my $i=0; $i < $numcolfixo; $i++) {
       
       if ($refstruc->[$i]->{VARIA} == 0) {
           #campos fixos
           $ini = $refstruc->[$i]->{DESLOC};           
           my @elemento = @$refbytes[$ini..($ini+$refstruc->[$i]->{TAM}-1)];

           #decodifica os bytes de acordo com o tipo de dado
           my $valor = &decodifica_valores($refstruc->[$i]->{TIPO}, \@elemento, $refstruc->[$i]->{ESCALA});
           
           #grava o valor decodificado
           $coluna->{$refstruc->[$i]->{NOME}}=$valor;

       }
       
   }

   #inicio do index das colunas variaveis
   my $indexvar = $offnumcol+$bytesbitmap+4;
   my $fim = 0;

   #percorre colunas variaveis
   $ini = $indexvar+ 2*$numcolvar;  

   my $i=$numcolfixo;
   foreach $elem (@$refstruc) {

       if ($elem->{VARIA} == 1) {
           #campos variaveis

           #indice para a posicao final do campo variavel
           $fimpos = $indexvar + 2*($i-$numcolfixo);

           my $valor=0;

           if (($fimpos+1) < scalar(@$refbytes)) {
              #posicao final do campo variavel
              $fim = (($refbytes->[$fimpos+1] << 8) + $refbytes->[$fimpos])-1; 
                              
              #pega os bytes relativos a coluna
              my @elemento = @$refbytes[$ini..$fim];
       
              #decodifica os bytes de acordo com o tipo de dado
              $valor = &decodifica_valores($elem->{TIPO}, \@elemento, $refstruc->[$i]->{ESCALA});
              
           }
          
           #grava o valor decodificado
           $coluna->{$elem->{NOME}}=$valor;

           #inicio do novo campo
           $ini = $fim+1;

           $i++;

           #termina se acabaram as colunas
           last unless ($i < $numcol);

       }       
   }
}


#-----------------------------------------------------------------------------------------------------

sub captavaloresUPD {
   my $ultlinha = shift;
   my $refstruc = shift;
   my $row0 = shift;
   my $row1 = shift;
   my $offset = shift;
   my $localizacao = shift;
   my $modificacoes = shift;
   my $chaves = shift;


   my @bytesrow0 = unpack("C*", $row0);
   my @bytesrow1 = unpack("C*", $row1);

   #linha de referencia
   my @bytes = @{$ultlinha->{$localizacao}};

   #valores em bytes antes da alteracao
   my @valantes = @bytes[0..($offset-1)];
   push(@valantes, @bytesrow0);

   #valores em bytes depois da alteracao
   my @valdepois = @bytes[0..($offset-1)];
   push(@valdepois, @bytesrow1);

   #eh necessario complementar a linha pq os campos row0 e row1 nao trazem tudo 
   my $maior = scalar(@valantes);
   $maior = scalar(@valdepois) if (scalar(@valdepois) > $maior);

   if (scalar(@valantes) < scalar(@bytes)) {
      my @aux = @bytes[$maior..(scalar(@bytes)-1)];
      push(@valantes, @aux);
   }

   if (scalar(@valdepois) < scalar(@bytes)) {
      my @aux = @bytes[$maior..(scalar(@bytes)-1)];
      push(@valdepois, @aux);
   }
   
   #decodifica os valores antes da alteracao
   my %cols_antes = ();
   &decodifica_colunas(\@valantes, $refstruc, \%cols_antes);


   #decodifica os valores depois da alteracao
   my %cols_dep = ();
   &decodifica_colunas(\@valdepois, $refstruc, \%cols_dep);

   #verifica quais colunas mudaram de valor
   foreach (keys %cols_antes) {
      if (!defined($cols_dep{$_})) {
         push @$modificacoes, "Coluna $_ removida ou null";        
      } 
      else {
         push @$modificacoes, "Coluna $_: de $cols_antes{$_} para $cols_dep{$_}" if (&diferente($cols_antes{$_}, $cols_dep{$_}))
      }
   }

   foreach (keys %cols_dep) {
      if ((!defined($cols_antes{$_})) && (defined($cols_dep{$_}))) {
         push @$modificacoes, "Coluna $_ de null para $cols_dep{$_}";
      }      
   }

   #atribui os valores chaves
   foreach (keys %{$chaves}) {$chaves->{$_} = "$_ = $cols_antes{$_}";}

   #retrocede a linha de referencia para essa pagina-slot, 
   #ja que estamos lendo as transacoes na ordem descendente
   $ultlinha->{$localizacao} = \@valantes;

   #foreach (keys %cols_antes)   {print "$_: $cols_antes{$_} ";}
   #print "\n";
   #foreach (keys %cols_dep)   {print "$_: $cols_dep{$_} ";}
   #print "\n";
   

}


#-----------------------------------------------------------------------------------------------------

sub captavaloresINSDEL {
   my $ultlinha = shift;
   my $refstruc = shift;
   my $row0 = shift;
   my $localizacao = shift;
   my $modificacoes = shift;
   my $chaves = shift;


   my @bytesrow0 = unpack("C*", $row0);

   #decodifica os valores depois da alteracao
   my %cols_dep = ();
   &decodifica_colunas(\@bytesrow0, $refstruc, \%cols_dep);

   foreach (keys %cols_dep) {
      if (defined($cols_dep{$_})) {
         push @$modificacoes, "Coluna $_ : $cols_dep{$_}";
      }      
   }

   #atribui os valores chaves
   foreach (keys %{$chaves}) {$chaves->{$_} = "$_ = $cols_dep{$_}";}

   #retrocede a linha de referencia para essa pagina-slot, 
   #ja que estamos lendo as transacoes na ordem descendente
   $ultlinha->{$localizacao} = \@bytesrow0;

   #foreach (keys %cols_antes)   {print "$_: $cols_antes{$_} ";}
   #print "\n";
   #foreach (keys %cols_dep)   {print "$_: $cols_dep{$_} ";}
   #print "\n";
   

}


#-----------------------------------------------------------------------------------------------------



sub captalinhasOper() {
   my $pID = shift;
   my $banco = shift;
   my $server = shift;
   my $ultlinha = shift;

   my $Conn = Win32::OLE->new("ADODB.Connection");
   my $RS   = Win32::OLE->new("ADODB.Recordset"); 

   my $DSN = "Provider=SQLNCLI;Server=$server;Database=$banco;Trusted_Connection=yes;";

   #transaction ID
   my $trID = $pID->{TID};

   $Conn->Open($DSN);


   #transacoes de update e insert, menos DDL, na ordem inversa que ocorreram
   my $SQL = "Select [Page ID], [Slot ID], [Offset in Row], [RowLog Contents 0], [RowLog Contents 1], Operation " .
             "from ::fn_dblog(null,null) where [Transaction ID] = \'$trID\' and " .
             "(Operation in ('LOP_MODIFY_ROW', 'LOP_INSERT_ROWS', 'LOP_MODIFY_COLUMNS', 'LOP_DELETE_ROWS' )) and (Context in ('LCX_CLUSTERED', 'LCX_MARK_AS_GHOST')) and " . 
             "(AllocUnitName not like 'sys.%') order by [Current LSN] DESC";


   $RS->Open($SQL, $Conn, 1, 1);


   until ($RS->EOF) {

      my ($pageIDa, $pageIDb, $slotID, $offrow, @row0, $row1, $operacao, $tipo);

      ($pageIDa, $pageIDb) = split(/\:/,$RS->Fields("Page ID")->value);
   
      $slotID = $RS->Fields("Slot ID")->value;
      $offrow = $RS->Fields("Offset in Row")->value; 
      $row0 = $RS->Fields("RowLog Contents 0")->value;
      $row1 = $RS->Fields("RowLog Contents 1")->value;
      $operacao = $RS->Fields("Operation")->value;      

      #converte de hexa para decimal
      $pageIDa = hex($pageIDa);
      $pageIDb = hex($pageIDb);

      #precisa usar o sqlcmd pq o resultado nao vem como tabela
      my $resp2 = &execquery("dbcc traceon(3604) WITH NO_INFOMSGS; dbcc page($banco, $pageIDa, $pageIDb, 3) WITH NO_INFOMSGS", $servidor); 

      #pega o nome e o ID da tabela
      my ($tabID, $tabnome) = &captatab($resp2, $banco, $servidor);

      #estruturas para armazenar os resultados
      my %chaves = ();
      my @modificacoes = ();
     
      #mapeia a tabela e suas colunas
      my @structable = ();
      &captaestruturatabela($tabID, $banco, $servidor, \@structable);

      my $localizacao = "$pageIDa:$pageIDb-$slotID";

      #pega as chaves da linha alterada
      &captachaves($tabID, $banco, $servidor, \%chaves, $slotID, $resp2);

      #pega a string de bytes da linha alterada      
      if (!defined($ultlinha->{$localizacao})) {  
         my @auxlinha = ();        
         @auxlinha = &stringtoarray(&captabytes($resp2, $slotID)) if ($operacao =~ /MODIFY/);            
         $ultlinha->{$localizacao} = \@auxlinha;
      }

 
      if (($operacao =~ /MODIFY/)) {

         $tipo = "UPDATE";
         
         #em alguns casos nao mapeados, o offset vem null -> LOP_MODIFY_COLUMN
         if ($offrow) {
          
            #mapeia os valores e as alteracoes dessa transacao
            &captavaloresUPD($ultlinha, \@structable, $row0, $row1, $offrow, $localizacao, \@modificacoes, \%chaves);

         }

      }
      elsif ($operacao =~ /(INSERT|DELETE)/) {
         $tipo = $1;

         #mapeia os valores e as alteracoes dessa transacao
         &captavaloresINSDEL($ultlinha, \@structable, $row0, $localizacao, \@modificacoes, \%chaves);

      }
      
      #monta registro com detalhes da operacao
      my $info = {
            TAB    => $tabnome,                          # nome da tabela
            TIPO   => $tipo,                             # UPDATE, INSERT ou DELETE
            CHAVES => \%chaves,                          # hash com as colunas chaves e valores, identificando a linha alterada
            MODIF  => \@modificacoes                     # updates nas colunas indicando de/para
      };

      #guarda os dados relativos a essa tabela
      push @{$pID->{DADOS}}, $info;
      
      $RS->MoveNext;
   }

   $RS->Close;
   $Conn->Close;

   
}


#-----------------------------------------------------------------------------------------------------

sub testacon {
  $Conn = Win32::OLE->new("ADODB.Connection");
  $RS   = Win32::OLE->new("ADODB.Recordset"); 

  $DSN = "Provider=SQLNCLI;Server=servidor;Database=banco;Trusted_Connection=yes;";


  $Conn->Open($DSN);


  $SQL =<<EOF;
SELECT * FROM ::fn_dblog(null,null)
EOF

  $RS->Open($SQL, $Conn, 1, 1);

until ($RS->EOF) {
   my $value = $RS->Fields("Page ID")->value;
   print "$value\n";
   $RS->MoveNext;
}

$RS->Close;
$Conn->Close;


}


#-----EOF-------
