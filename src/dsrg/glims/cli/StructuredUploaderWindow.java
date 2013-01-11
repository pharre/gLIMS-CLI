package dsrg.glims.gui;

import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.border.EmptyBorder;

public class JframeUploader extends JFrame {
	
	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	JFileChooser fileChooser;
	JButton openButton;
	JTextArea log;


	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					JframeUploader frame = new JframeUploader();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public JframeUploader() {
		setTitle("gLIMS Structured File Uplader");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 582, 523);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);
		
		JButton btnNewButton = new JButton("Select File");
		btnNewButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				int returnVal = fileChooser.showDialog(contentPane, "File to upload");
				if (returnVal == JFileChooser.APPROVE_OPTION) {
					File fileName = fileChooser.getSelectedFile();
					log.append("You selected " + fileName.getName() + "\n");
					
					JOptionPane.showMessageDialog(contentPane, "Select google drive directory location");
					
					fileChooser.setSelectedFile(fileName);
					fileChooser.showDialog(contentPane, "Select root folder name");
					if (returnVal == JFileChooser.APPROVE_OPTION) {
						File newFile = fileChooser.getSelectedFile();
						log.append("Writing gLIMS files to your directory: " + newFile.getName());
						//dsrg.glims.cli.StructuredUploader();
					}
				}
			}
		});
		btnNewButton.setBounds(33, 49, 89, 23);
		contentPane.add(btnNewButton);
		
		fileChooser = new JFileChooser();
		fileChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
		fileChooser.setPreferredSize(new Dimension(900, 700));
		
		
		log = new JTextArea(5, 20);
		log.setMargin(new Insets(5, 5, 5, 5));
		log.setEditable(false);
	}
}
