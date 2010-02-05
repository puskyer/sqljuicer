#!/usr/bin/perl -w
# List database transactions using Transaction Log
# use: SQLJuicer.pl -s server -b database [-h]
# 
# -b :database name
# -s :sql server name
# -h :this help
# 
# Dependecies:
#       SQLCMD
#   
#
use Getopt::Std;
use Time::Local;
use Date::Calc qw(Add_Delta_Days);
use Win32::OLE;
use Win32::OLE::Const 'Microsoft ActiveX Data Objects';

my $ver="0.1";

#options
%args = ( );
getopts("b:s:h", \%args);

#write small help
if ($args{h}) {
   &myheader;
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

# ----- script var
my @tranDet;
my %dados;
my $banco = $args{b};
my $servidor = $args{s};

my %ultlinha = ( );


#--------------------------

&myheader;

#get each Transaction ID with commit
&getTransactionIDs(\@tranDet, $banco, $servidor);

#Loop through transactions commited, ordered by date/time 
#from last to first
foreach my $pID (@tranDet) {

   #get transaction datetime
   my $dtTr = $pID->{DATA};

   if ($dtTr) {

      #parse the operation
      &getOperations($pID, $banco, $servidor, \%ultlinha);

   }
}

#print results
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

#### END OF MAIN ROUTINE #####

####################################### PROCEDURES  ######################################################

sub myheader {
   print <<CABEC;

SQLJuicer.pl v$ver
Lista Transacoes do Transaction Log
http://code.google.com/p/sqljuicer/
--------------------------------------------------------------------------

CABEC
}



#-----------------------------------------------------------------------------------------------------

sub getTransactionIDs {

  my $p1 = shift;
  my $banco = shift;
  my $server = shift;


  my $Conn = Win32::OLE->new("ADODB.Connection");
  my $RS   = Win32::OLE->new("ADODB.Recordset"); 

  my $DSN = "Provider=SQLNCLI;Server=$server;Database=$banco;Trusted_Connection=yes;";


  $Conn->Open($DSN);


  #get all commited transaction IDs ordered by datetime from last to first
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

#---------------------------------- UTILITIES ------------------------------------------------------

sub execquery {
#exec queries using sqlcmd

  my $cmd = shift;
  my $server = shift;

  my $comstr = "sqlcmd -S $server -E -Q \"$cmd\"";

  my $resp = `$comstr`;

  return $resp;
}

#-----------------------------------------------------------------------------------------------------

sub getbytesfromDBCC {
#parse dbcc page results extracting bytes related to pageID and Slot (parameters)

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
# transform string of bytes in array of bytes

   my $bytes = shift;

   my @byteslinha=();
   while ($bytes =~ /([0-9A-Fa-f]{2})/g) {$byteslinha[scalar(@byteslinha)]=hex($1);};   

   return @byteslinha;
}

#-----------------------------------------------------------------------------------------------------

sub bytetoword {
# transform array of bytes in array of words (unsigned small int)

   my (@wordaux) = @_;  
   
   #adiciona um elemento 0 no final se tem numero impar de bytes
   $wordaux[scalar(@wordaux)] = 0 if (scalar(@wordaux) % 2 == 1);

   #separa em unsigned integer, invertendo o little endian
   my @wordslinha=();

   for (my $i = 1; $i <= (scalar(@wordaux)/2); $i++) {$wordslinha[scalar(@wordslinha)]=($wordaux[2*$i-1] << 8) + $wordaux[2*$i-2];}                

   return @wordslinha;
}

#-----------------------------------------------------------------------------------------------------

sub IsDifferent {
# compare parameters (numbers or strings), returning true if they are different

   my ($a, $b) = (@_);

   if ($a =~ /^\d+\.?\d+$/) {return ($a!=$b);}
   else {return ($a ne $b)}

}

#-----------------------------------------------------------------------------------------------------

sub parseObjectID {
# parse ObjectID of table affected in transaction from dbcc result

   my $ent = shift;
   my $banco = shift;
   my $server = shift;

   my ($tabID) = ($ent =~ /Metadata\: ObjectId \=\s+(\d+)/s);

   return ($tabID, &getTableName($tabID, $banco, $server));

}

#----------------------------------- UTILITARIOS DE BANCO -----------------------------------------------------------------

sub getKeyColumns {
# get key columns of the affected table

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


}

#-----------------------------------------------------------------------------------------------------

sub getTableStructure {
# get table columns, types, etc

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

   #first fixed size column starts after 4 bytes
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

sub getTableName {
# get table name using the object ID

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

sub decodeValue {
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
         #byte at offset 0 always has 01, so it's jumped ->>> NOT SURE, IT WORKED WELL IN ALL TESTS SO FAR
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

      #string unicode format
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
           
      #string in HEX
      foreach (@$refvalores) {$valor = $valor . sprintf("%x",$_)}
   
      $valor = unpack "d", pack "H*", $valor;   

   
   }
   else {
      # message for not translated column types
      $valor = "Tipo $tipo valor=@$refvalores"; 
   }

   return $valor;        

}

#-----------------------------------------------------------------------------------------------------

sub extractRawColumns {
# take all columns and extract its raw contents

   my $refbytes = shift;
   my $refstruc = shift;
   my $coluna = shift;

   #column count offset
   my $offnumcol = ($refbytes->[3] << 8) + $refbytes->[2];

   #column count
   my $numcol = ($refbytes->[$offnumcol+1] << 8) + $refbytes->[$offnumcol];

   my ($bytesbitmap, $valor);

   #how many bytes for null bitmap
   {use integer;
     $bytesbitmap = ($numcol/8) + (($numcol % 8) == 0 ? 0: 1);
   }

   #how many variable size columns
   my $numcolvar = (scalar(@$refbytes) > ($offnumcol+3+$bytesbitmap)) ? ($refbytes->[$offnumcol+3+$bytesbitmap] << 8) + $refbytes->[$offnumcol+$bytesbitmap+2] : 0;

   #how many fixed size columns
   my $numcolfixo = $numcol-$numcolvar;

   my $ini;

   #loop through fixed size columns
   for (my $i=0; $i < $numcolfixo; $i++) {
       
       if ($refstruc->[$i]->{VARIA} == 0) {
           #fixed size column
           $ini = $refstruc->[$i]->{DESLOC};           
           my @elemento = @$refbytes[$ini..($ini+$refstruc->[$i]->{TAM}-1)];

           #Send raw bytes to decode according to the column type
           my $valor = &decodeValue($refstruc->[$i]->{TIPO}, \@elemento, $refstruc->[$i]->{ESCALA});
           
           #save decoded value
           $coluna->{$refstruc->[$i]->{NOME}}=$valor;

       }
       
   }

   #variable size columns index offset
   my $indexvar = $offnumcol+$bytesbitmap+4;
   my $fim = 0;

   #loop through variable size columns
   $ini = $indexvar+ 2*$numcolvar;  

   my $i=$numcolfixo;
   foreach $elem (@$refstruc) {

       if ($elem->{VARIA} == 1) {
           #variable columns

           #end position of this column - offset
           $fimpos = $indexvar + 2*($i-$numcolfixo);

           my $valor=0;

           if (($fimpos+1) < scalar(@$refbytes)) {
              #end position of this column
              $fim = (($refbytes->[$fimpos+1] << 8) + $refbytes->[$fimpos])-1; 
                              
              #extract raw bytes
              my @elemento = @$refbytes[$ini..$fim];
       
              #Send raw bytes to decode according to the column type
              $valor = &decodeValue($elem->{TIPO}, \@elemento, $refstruc->[$i]->{ESCALA});
              
           }
          
           #save decoded value
           $coluna->{$elem->{NOME}}=$valor;

           #start of next column
           $ini = $fim+1;

           $i++;

           #no more columns then exit loop
           last unless ($i < $numcol);

       }       
   }
}


#-----------------------------------------------------------------------------------------------------

sub parseUpdateTrans {
#get values involved in a update operation, before and after

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

   #reference row
   my @bytes = @{$ultlinha->{$localizacao}};

   #raw bytes before update
   my @valantes = @bytes[0..($offset-1)];
   push(@valantes, @bytesrow0);

   #raw bytes after update
   my @valdepois = @bytes[0..($offset-1)];
   push(@valdepois, @bytesrow1);

   #row0 and row1 columns do not have complete row. We must complete them
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
   
   #Decode values before update
   my %cols_antes = ();
   &extractRawColumns(\@valantes, $refstruc, \%cols_antes);


   #Decode values after update
   my %cols_dep = ();
   &extractRawColumns(\@valdepois, $refstruc, \%cols_dep);

   #compare them to check what has changed
   foreach (keys %cols_antes) {
      if (!defined($cols_dep{$_})) {
         push @$modificacoes, "Coluna $_ removida ou null";        
      } 
      else {
         push @$modificacoes, "Coluna $_: de $cols_antes{$_} para $cols_dep{$_}" if (&IsDifferent($cols_antes{$_}, $cols_dep{$_}))
      }
   }

   foreach (keys %cols_dep) {
      if ((!defined($cols_antes{$_})) && (defined($cols_dep{$_}))) {
         push @$modificacoes, "Coluna $_ de null para $cols_dep{$_}";
      }      
   }

   #save the key columns
   foreach (keys %{$chaves}) {$chaves->{$_} = "$_ = $cols_antes{$_}";}

   #make the current row the reference row for the prior transaction
   #this is the reason we are reading transactions from last to first
   $ultlinha->{$localizacao} = \@valantes;

   #just for test
   #foreach (keys %cols_antes)   {print "$_: $cols_antes{$_} ";}
   #print "\n";
   #foreach (keys %cols_dep)   {print "$_: $cols_dep{$_} ";}
   #print "\n";
   

}


#-----------------------------------------------------------------------------------------------------

sub parseINSDELTrans {
#get the values for INSERT and DELETE operations

   my $ultlinha = shift;
   my $refstruc = shift;
   my $row0 = shift;
   my $localizacao = shift;
   my $modificacoes = shift;
   my $chaves = shift;


   my @bytesrow0 = unpack("C*", $row0);

   #decode raw bytes after the operation
   my %cols_dep = ();
   &extractRawColumns(\@bytesrow0, $refstruc, \%cols_dep);

   foreach (keys %cols_dep) {
      if (defined($cols_dep{$_})) {
         push @$modificacoes, "Coluna $_ : $cols_dep{$_}";
      }      
   }

   #save the key columns
   foreach (keys %{$chaves}) {$chaves->{$_} = "$_ = $cols_dep{$_}";}

   #make the current row the reference row for the prior transaction
   #this is the reason we are reading transactions from last to first
   $ultlinha->{$localizacao} = \@bytesrow0;

   #just for tests
   #foreach (keys %cols_antes)   {print "$_: $cols_antes{$_} ";}
   #print "\n";
   #foreach (keys %cols_dep)   {print "$_: $cols_dep{$_} ";}
   #print "\n";
   

}


#-----------------------------------------------------------------------------------------------------
sub getOperations() {
#parse transaction rows of transaction log

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


   #get update, insert and delete, DML only, ordered by datetime from last to first
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

      #hex to decimal
      $pageIDa = hex($pageIDa);
      $pageIDb = hex($pageIDb);

      #need to query using sqlcmd b/c result is not a recordset. I'm not sure if its possible with OLEDB
      my $resp2 = &execquery("dbcc traceon(3604) WITH NO_INFOMSGS; dbcc page($banco, $pageIDa, $pageIDb, 3) WITH NO_INFOMSGS", $servidor); 

      #get table ID and name
      my ($tabID, $tabnome) = &parseObjectID($resp2, $banco, $servidor);

      #results will be saved in these structs
      my %chaves = ();
      my @modificacoes = ();
     
      #map table and columns of transaction
      my @structable = ();
      &getTableStructure($tabID, $banco, $servidor, \@structable);

      my $localizacao = "$pageIDa:$pageIDb-$slotID";

      #get key columns 
      &getKeyColumns($tabID, $banco, $servidor, \%chaves, $slotID, $resp2);

      #get the raw bytes of the reference row
      if (!defined($ultlinha->{$localizacao})) {  
         my @auxlinha = ();        
         @auxlinha = &stringtoarray(&getbytesfromDBCC($resp2, $slotID)) if ($operacao =~ /MODIFY/);            
         $ultlinha->{$localizacao} = \@auxlinha;
      }

 
      if (($operacao =~ /MODIFY/)) {

         $tipo = "UPDATE";
         
         #in some cases (LOP_MODIFY_COLUMN), offrow will be null !!! WHY ???
         if ($offrow) {
          
            #map the transaction
            &parseUpdateTrans($ultlinha, \@structable, $row0, $row1, $offrow, $localizacao, \@modificacoes, \%chaves);

         }

      }
      elsif ($operacao =~ /(INSERT|DELETE)/) {
         $tipo = $1;

         #map the transaction
         &parseINSDELTrans($ultlinha, \@structable, $row0, $localizacao, \@modificacoes, \%chaves);

      }
      
      #save details in the struct
      my $info = {
            TAB    => $tabnome,                          # table name
            TIPO   => $tipo,                             # UPDATE, INSERT or DELETE
            CHAVES => \%chaves,                          # perl hash with key columns and values of affected row
            MODIF  => \@modificacoes                     # row updates showing pairs from value/to value
      };

      #save this struct
      push @{$pID->{DADOS}}, $info;


      #next transaction       
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
