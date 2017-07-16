package com.ht.dev.rppsimport;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Classe de gestion de la base de données.
 * @author tondeur-h
 */
public class DataBase {

    private Connection connexion;
    private Statement stat;
    private ResultSet result;
    private CallableStatement callableStat;

    private String urlDB, userDB, passwordDB;
    private String driverDB;

//logger par les api java
    static final Logger logger = Logger.getLogger(DataBase.class.getName());

    public DataBase(String driver) {
        //chargement du drivers DB
        try {
            driverDB = driver;
            //charger le drivers DB pour toute la session Scandoc
            Class.forName(driverDB);
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }

        //parametrage du logger
        try {
            logger.setUseParentHandlers(false);
            FileHandler fh = new FileHandler("rppsImportDB%u.log", 0, 1, true);
            fh.setFormatter(new SimpleFormatter());
            logger.addHandler(fh);
        } catch (IOException ioe) {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ioe);
        }

    } //fin du constructeur

    /**
     * *******************************************************
     * Connection a la base de données des patients
     *
     * @param jdbcurl String
     * @param dbuser String
     * @param dbpwd String
     * @return boolean
     *
     ********************************************************
     */
    public boolean connect_db(String jdbcurl, String dbuser, String dbpwd) {
        setUrl(jdbcurl);
        setUser(dbuser);
        setPassword(dbpwd);

        try {
            connexion = DriverManager.getConnection(getUrl(), getUser(), getPassword());
        } catch (SQLException ex) {
            //ex.printStackTrace();
            logger.log(Level.SEVERE, "Erreur connection DB", ex);
            return false;
        }
        return true;
    } //fin connect_db

    
    /****************************************
     * Requete standard
     * @param sql
     * @return 
     ****************************************/
    public ResultSet query(String sql) {
        try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            return result;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete DB", ex);
        }
        return null;
    }

    
    /********************************
     * REquete update ou insert
     * @param sql
     * @return 
     ********************************/
    public int update(String sql) {
        try {
            stat = connexion.createStatement();
            return stat.executeUpdate(sql);
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete DB", ex);
        }
        return -1;
    }

/******************
 * Batching Update
 *****************/ 
    public void prepareBatch(){
        try {
            connexion.setAutoCommit(false);
            stat=connexion.createStatement();
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    public void updateBatch(String sql){
        try {
            stat.addBatch(sql);
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
    
    public long[] batchExec (int size){
        long[] rep=new long[size];
        
        try {
            rep=stat.executeLargeBatch();
            connexion.commit();
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return rep;
    }
    
    /**
     * *************************************
     * retourne le resulset courant ou null
     *
     * @return Resultset
      **************************************
     */
    public ResultSet getResultSet() {
        return result;
    }

    /**
     * *************************************
     * Fermer le resulset
      **************************************
     */
    public void closeResultSet() {
        try {
            if (result != null) {
                result.close();
            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    /**
     * *************************************
     * Fermer le Statement
      **************************************
     */
    public void closeStatement() {
        try {
            if (stat != null) {
                stat.close();
            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    /**
     * *************************************
     * Fermer le CallStatement
     **************************************
     */
    public void closeCallStatement() {
        try {
            if (callableStat != null) {
                callableStat.close();
            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    /**
     * *************************************
     * Fermer la connexion
    **************************************
     */
    public void closeConnexion() {
        try {
            if (connexion != null) {
                connexion.close();
            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    /**
     * ************************************************************
     * Fermer la connection base de données (tout)
     *************************************************************
     */
    public void close_db() {
        closeResultSet();
        closeStatement();
        closeCallStatement();
        closeConnexion();
    } // fin clode_db

    
    /**
     * ****************
     * @return the urlDB
     *****************
     */
    public String getUrl() {
        return urlDB;
    }

    
    /**
     * **************************
     * @param url the urlDB to set
     ***************************
     */
    public void setUrl(String url) {
        this.urlDB = url;
    }

    
    /**
     * *****************
     * @return the userDB
     ******************
     */
    public String getUser() {
        return userDB;
    }

    
    /**
     * ***************************
     * @param user the userDB to set
     ****************************
     */
    public void setUser(String user) {
        this.userDB = user;
    }

    
    /**
     * @return the passwordDB
     */
    public String getPassword() {
        return passwordDB;
    }

    /**
     * @param password the passwordDB to set
     */
    public void setPassword(String password) {
        this.passwordDB = password;
    }

    /**
     * @return the driverDB
     */
    public String getDriverDB() {
        return driverDB;
    }

   //==============PROCEDURES===================
    
    /****************************************
     * Rechercher un document par son numero
     * @param NUMDOC
     * @return 
     ****************************************/
    public ResultSet CallGETDOCUMENT(String NUMDOC) {
        String sql="SELECT NOMDOCUMENT FROM TBLDOCUMENTS WHERE NUMDOCEXTERNE='"+NUMDOC+"'";
          try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            stat.close();
            return result;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete GETDOCUMENT", ex);
        }
        return null;
    }


    /****************************************
     * Lister les docuents de la BAL ORU
     * @return 
     ****************************************/
    public ResultSet CallLISTEBALDOC() {
        String sql="SELECT " +
                    "pat.etc_nom," +
                    "pat.etc_prn," +
                    "pat.etc_nom_mar," +
                    "pat.etc_ddn," +
                    "pat.etc_sex," +
                    "doc.IPP," +
                    "doc.IEP," +
                    "doc.idlocalisation," +
                    "date_format( doc.datecreation,'%Y-%m-%d %H:%i:%s')," +
                    "doc.aliasdoc," +
                    "doc.descrdoc," +
                    "doc.auteur," +
                    "doc.numdocexterne," +
                    "doc.nomdocument," +
                    "bal.IDMESSAGE," +
                    "doc.idetatdoc, " +
                    "bal.iddocument " +
                    "FROM tblbal_doc bal, tbldocuments doc, pa_pat pat " +
                    "WHERE bal.GEN='0' " +
                    "AND bal.IDDOCUMENT=doc.IDDOCUMENT " +
                    "and doc.ipp= pat.pat_ipp";
          try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            stat.close();
            return result;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete CallListBalDocORU", ex);
        }
        return null;
    }

    
      /****************************************
     * Lister les docuents de la BAL MDM
     * @return 
     ****************************************/
    public ResultSet CallLISTEBALDOCMDM() {
        String sql="SELECT " +
                    "pat.etc_nom," +
                    "pat.etc_prn," +
                    "pat.etc_nom_mar," +
                    "pat.etc_ddn," +
                    "pat.etc_sex," +
                    "doc.IPP," +
                    "doc.IEP," +
                    "doc.idlocalisation," +
                    "date_format( doc.datecreation,'%Y-%m-%d %H:%i:%s')," +
                    "doc.aliasdoc," +
                    "doc.descrdoc," +
                    "doc.auteur," +
                    "doc.numdocexterne," +
                    "doc.nomdocument," +
                    "bal.IDMESSAGE," +
                    "doc.idetatdoc, " +
                    "bal.iddocument " +
                    "FROM tblbal_doc bal, tbldocuments doc, pa_pat pat " +
                    "WHERE bal.GENMDM='0' " +
                    "AND bal.IDDOCUMENT=doc.IDDOCUMENT " +
                    "and doc.ipp= pat.pat_ipp";
          try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            stat.close();
            return result;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete CalllistBalDocMDM", ex);
        }
        return null;
    }

    
    
       /****************************************
     * Lister les docuents de la BAL ORU
     * @param nummessage
     ****************************************/
    public void CallVALIDEBALDOC ( int nummessage ) {
        String sql="UPDATE TBLBAL_DOC SET gen='1', dategen=sysdate() WHERE idmessage='"+nummessage+"'";
          try {
            stat =connexion.createStatement();
            stat.executeUpdate(sql);
            stat.close();
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete CallValiderBalDocORU", ex);
        }
    }
    
    
      /****************************************
     * Lister les docuents de la BAL MDM
     * @param nummessage
     ****************************************/
    public void CallVALIDEBALDOCMDM ( int nummessage ) {
        String sql="UPDATE TBLBAL_DOC SET genMDM='1', dategenMDM=sysdate() WHERE idmessage='"+nummessage+"'";
          try {
            stat =connexion.createStatement();
            stat.executeUpdate(sql);
            stat.close();
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete CalValiderBalDocMDM", ex);
        }
    }  
    
    /****************************************
     * Liste des commentaire par filtre
     * @param filtreP
     * @return 
     ****************************************/
    public ResultSet CallLISTCOMMENTAIRE(String filtreP) {
        String sql="SELECT COMMENTAIRE FROM TBLTYPECOM WHERE FILTRE like '%"+filtreP+"%' ORDER BY ORDRE ASC";
          try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            stat.close();
            return result;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete LISTCOMMENTAIRE", ex);
        }
        return null;
    }

   
       /****************************************
     * Liste des commentaire par filtre
     * @param tpdoc
     * @return 
     ****************************************/
    public ResultSet CallLISTCOMMENTAIRE16(String tpdoc) {
        String sql="FOR SELECT COMMENTAIRE FROM TBLTYPECOM WHERE TYPEDOC='"+tpdoc+"' ORDER BY ORDRE ASC";
          try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            stat.close();
            return result;
          } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete LISTCOMMENTAIRE", ex);
        }
        return null;
    }
    

    /****************************************
     * Liste des types par filtre
     * @param filtreP
     * @return 
     ****************************************/
    public ResultSet CallLISTTYPEDOC(String filtreP) {
        String sql="SELECT ALIASDOC, LIBELLETYPEDOC FROM TBLTYPEDOC WHERE FILTRE like '%"+filtreP+"%' ORDER BY IDTYPEDOC ASC";
          try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            stat.close();
            return result;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete LISTCOMMENTAIRE", ex);
        }
        return null;
    }

   
    /****************************************
     * Liste des types par filtre
     * @param filtreP
     * @return 
     ****************************************/
    public ResultSet CallLISTTYPEDOC16(String filtreP) {
        String sql="SELECT ALIASDOC, LIBELLETYPEDOC,IDTYPEDOC FROM TBLTYPEDOC WHERE FILTRE like '%"+filtreP+"%'  ORDER BY ORDRE ASC";
          try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            stat.close();
            return result;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete LISTCOMMENTAIRE", ex);
        }
        return null;
    }

    
    /**********************************
     * Mettre a jour un document par 
     * son numéro
     * @param NUMDOC 
     **********************************/
    void CallUPDATEDOC(String NUMDOC) 
    {
        try {
            String sql="select IDDOCUMENT into iddoc from TBLDOCUMENTS WHERE NUMDOCEXTERNE="+NUMDOC;
            String iddoc=query(sql).getString(1);
            closeStatement();
            sql="UPDATE TBLDOCUMENTS set datestockage=sysdate() WHERE NUMDOCEXTERNE="+NUMDOC;
            update(sql);
            closeStatement();
            sql="UPDATE TBLBAL_DOC SET DATEGEN=NULL, GEN='0' WHERE IDDOCUMENT="+iddoc;
            update(sql);
            closeStatement();
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    
    /********************************
     * 
     * @param IPP
     * @param IEP
     * @param UF
     * @param NUMDOC
     * @param string
     * @param ALIAS
     * @param DESC
     * @param date_hl7_to_datesys
     * @param AUTEUR
     * @return 
     ********************************/
    boolean CallINSERTDOC(String vipp, String viep, String vuf, String vnumdoc, String vnomdoc, String valias, String vdescr, String vdatecreat, String vauteur) 
    {
        try 
        {
            String sql="INSERT INTO tbldocuments(IPP,IEP, numdocexterne, nomdocument, idlocalisation, auteur, idetatdoc, aliasdoc, descrdoc, datecreation, datestockage) "
                    + "VALUES ('"+ vipp +"','"+ viep +"','"+ vnumdoc +"','"+ vnomdoc +"','"+ vuf +"','"+ vauteur +"','0','"+ valias +"','"+ vdescr +"',str_to_date('"+ vdatecreat +"','%d/%m/%Y  %H:%i:%s'), sysdate())";
            update(sql);
            closeStatement();

            sql="SELECT LAST_INSERT_ID()"; //a placer dans iddoc;
            String iddoc=query(sql).getString(1);
            closeStatement();
            
            sql="INSERT INTO TBLBAL_DOC(iddocument, dategen, gen) VALUES ('"+iddoc+"',null,'0')";
            update(sql);
            closeStatement();

        } catch (SQLException ex) 
        {
            logger.log(Level.SEVERE, null, ex);
            return false;
        }
        
        return true;
    } //fin de la classe CallINSERTDOC

    
      /********************************
     * 
     * @param IPP
     * @param IEP
     * @param UF
     * @param NUMDOC
     * @param string
     * @param ALIAS
     * @param DESC
     * @param date_hl7_to_datesys
     * @param AUTEUR
     * @return 
     ********************************/
    boolean CallINSERTDOCSEC(String vipp, String viep, String vuf, String vnumdoc, String vnomdoc, String valias, String vdescr, String vdatecreat, String vauteur, String vETATDOC) 
    {
        try 
        {
            String sql="INSERT INTO tbldocuments(IPP,IEP, numdocexterne, nomdocument, idlocalisation, auteur, idetatdoc, aliasdoc, descrdoc, datecreation, datestockage) "
                    + "VALUES ('"+ vipp +"','"+ viep +"','"+ vnumdoc +"','"+ vnomdoc +"','"+ vuf +"','"+ vauteur +"','"+vETATDOC+"','"+ valias +"','"+ vdescr +"',str_to_date('"+ vdatecreat +"','%d/%m/%Y  %H:%i:%s'), sysdate())";
            update(sql);
            closeStatement();

            sql="SELECT LAST_INSERT_ID() as lastID FROM tbldocuments;"; //a placer dans iddoc;
            ResultSet r=query(sql);
            r.next();
            String iddoc=r.getString("lastID");
            closeStatement();
            
            sql="INSERT INTO TBLBAL_DOC(iddocument, dategen, gen) VALUES ('"+iddoc+"',null,'0')";
            update(sql);
            closeStatement();

        } catch (SQLException ex) 
        {
            logger.log(Level.SEVERE, null, ex);
            return true;
        }
        
        return false;
    } //fin de la classe CallINSERTDOC


    /****************************
     * Injecter les traces
     * @param commentaire
     * @param qui
     * @param numdoc 
     *****************************/
    void CallSETTRACECD(String commentaire, String qui, String numdoc) 
    {    
            String sql="INSERT INTO tbltraces(actiontrace, utilisateur,NUMDOC,Quand) VALUES ('"+commentaire+"','"+qui+"','"+numdoc+"',sysdate())";
            update(sql);
            closeStatement();

    } //fin de la classe CallSETTRACECD
    
   
    
    /***************************************
     * Getpatient par IEP
     * @param iep
     * @return 
     ****************************************/ 
    public ResultSet CallGETPATIENT(String iep) {
        String sql="SELECT " +
                    "P.IEP_IDE," +
                    "P.PAS_PER_DEB," +
                    "P.PAS_PER_FIN," +
                    "R.UFO_IDE," +
                    "R.UFO_IDE," +
                    "PA.PAT_IPP," +
                    "PA.ETC_NOM," +
                    "PA.ETC_PRN," +
                    "PA.ETC_NOM_MAR," +
                    "PA.ETC_DDN," +
                    "PA.ETC_SEX " +
                    "FROM PA_PAT PA, PA_PAS P, PA_RES_PAT R " +
                    "WHERE P.IEP_IDE='"+iep+"' " +
                    "AND PA.PAT_IPP=P.PAT_IPP " +
                    "AND P.PAS_IPS=R.PAS_IPS " +
                    "AND R.PA_RES_PAT_TYP='M' " +
                    "ORDER BY R.PA_RES_PAT_IDE DESC;";
          try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            stat.close();
            return result;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete GETPATIENT : "+iep, ex);
        }
        return null;
    }

    
      /***************************************
     * Getpatient par IPP
     * @param ipp
     * @return 
     ****************************************/ 
    public ResultSet CallGETSEJOURPATIENTBYIPP(String ipp) {
        String sql="SELECT P.IEP_IDE,DATE_FORMAT(P.PAS_PER_DEB,'%d/%m/%Y %H:%m') AS \"PAS_PER_DEB\",DATE_FORMAT(P.PAS_PER_FIN,'%d/%m/%Y %H:%m') AS \"PAS_PER_FIN\",R.CAC_IDE FROM PA_PAS P, PA_RES_PAT R WHERE P.PAT_IPP='"+ipp+"' AND R.PAS_IPS=P.PAS_IPS ORDER BY P.PAS_PER_DEB DESC";
          try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            stat.close();
            return result;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete GETSEJOURPATIENTBYIPP : "+ipp, ex);
        }
        return null;
    }

    
    
    /****************************************
     * GetPatient By all ipp, nom, prenom,ddn
     * @param i
     * @param n
     * @param p
     * @param d
     * @return 
     *****************************************/
    public ResultSet CallGETPATIENTBYALL(String i, String n, String p, String d) {
        String sql="SELECT P.PAT_IPP,P.ETC_NOM,P.ETC_NOM_MAR,P.ETC_PRN,DATE_FORMAT(P.ETC_DDN,'%d/%m/%Y') FROM PA_PAT P WHERE P.pat_ipp like '%"+i+"%' AND (P.etc_nom like '%"+n.toUpperCase()+"%' OR P.etc_nom_mar like '%"+n.toUpperCase()+"%') AND P.etc_ddn='"+d+"' AND P.etc_prn like '%"+p.toUpperCase()+"%'";
          try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            stat.close();
            return result;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete GETPATIENTBYALL : "+i+" "+n+" "+p+" "+d, ex);
        }
        return null;
    }

    
    /***************************************
     * GetPatient par ipp nom prenom
     * @param i
     * @param n
     * @param p 
     * @return  
     ***************************************/
    public ResultSet CallGETPATIENTBYNAMEONLY(String i, String n, String p) {
      String sql="SELECT P.PAT_IPP,P.ETC_NOM,P.ETC_NOM_MAR,P.ETC_PRN,DATE_FORMAT(P.ETC_DDN,'%d/%m/%Y') AS \"ETC_DDN\" FROM PA_PAT P WHERE P.pat_ipp like '%"+i+"%' AND (P.etc_nom like '%"+n.toUpperCase()+"%' OR P.etc_nom_mar like '%"+n.toUpperCase()+"%') AND P.etc_prn like '%"+p.toUpperCase()+"%'";
          try {
            stat = connexion.createStatement();
            result = stat.executeQuery(sql);
            stat.close();
            return result;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, "Erreur requete GETPATIENTBYALL : "+i+" "+n+" "+p, ex);
        }
        return null;
    }
    
}