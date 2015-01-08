package dblab.shop.transactions.sql;

import java.sql.SQLException;

/**
 * Dieses Interface muss von Klassen, die eine Verbindung von einer SQLConnector
 * Instanz erfragen, implementiert werden.
 * 
 * Der Zweck ist die Vereinbarung eines Rückrufes (Callback), 
 * falls die SQL Verbindung geschlossen wird.
 */
public interface SQLConnectorClient {

	/**
	 * Implementierungen dieser Methode sollten alle SQL Ressourcen freigeben,
	 * in der Regel handelt es sich dabei um PreparedStatement Instanzen.
	 * 
	 * @throws SQLException
	 * 		SQLExceptions werden weitergegeben, da die Tragweite eines Fehlers
	 * 		auf dieser Schicht nicht bekannt ist
	 */
	public void close() throws SQLException;
}
