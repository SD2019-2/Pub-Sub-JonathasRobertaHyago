package com.sd.rmiclient;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.*;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.ServerSocket;
import javax.swing.JOptionPane;

import com.sd.rmiinterface.RMIInterface;

public class ClientOperation {
	private static RMIInterface look_up;

	public static void main(String[] args) throws MalformedURLException, RemoteException, NotBoundException {
		String meuIp = "";
		String tipo = "";
		Map<String, ArrayList<String>> mapaTopicosDoBroker = new HashMap<String, ArrayList<String>>();
		Integer i = 1;

		ArrayList<String> topicosListSubscriber1 = new ArrayList();
		ArrayList<String> topicosListSubscriber2 = new ArrayList();

		topicosListSubscriber1.add("entreterimento");
		topicosListSubscriber1.add("futebol");

		topicosListSubscriber2.add("lifestyle");
		topicosListSubscriber2.add("noticias");
		topicosListSubscriber2.add("gamers");

		// Se comunica com o servidor de nomes para fazer o pub/sub
		look_up = (RMIInterface) Naming.lookup("//localhost/MyServer");

		Integer porta = 12345;
		createBroker(String.valueOf(porta), "C", "C" + i);
		i++;

		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createBroker(String.valueOf(++porta), "C", "C" + i);
		i = 1;

		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createBroker(String.valueOf(++porta), "E", "E" + i);
		i++;

		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createBroker(String.valueOf(++porta), "E", "E" + i);
		i = 1;

		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createSubscriber(topicosListSubscriber1, String.valueOf(++porta), "S" + i);
		i++;

		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createSubscriber(topicosListSubscriber2, String.valueOf(++porta), "S" + i);
		i = 1;

		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createPublisher("futebol", " Hoje tem gol do Gabigol.", String.valueOf(++porta), "P" + i);
		i++;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createPublisher("futebol", " Flamengo campeao da libertadores 2019.", String.valueOf(++porta), "P" + i);
		i++;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createPublisher("noticias", " Baleia Baleia Baleia", String.valueOf(++porta), "P" + i);
		i++;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createPublisher("lifestyle", " Crossfit e coaching a nova sensação do momento.", String.valueOf(++porta),
				"P" + i);
		i++;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createPublisher("futebol", " Até o flamengo venceu tudo e palmeiras ainda não tem mundial.",
				String.valueOf(++porta), "P" + i);
		i++;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createPublisher("gamers", " RGB, saiba como ganhar FPS nos jogos com luizinhas no gabinete.",
				String.valueOf(++porta), "P" + i);
		i++;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}
		createPublisher("entreterimento", " Kpop, nova moda adolescente, saiba como virar um Army.",
				String.valueOf(++porta), "P" + i);
		i++;

	}
	///////////////////////////////////

	public static void createBroker(String porta, String tipo, String nome) {
		new Thread() {
			public void run() {
				try {
					Map<String, ArrayList<String>> mapaTopicosDoBroker = new HashMap<String, ArrayList<String>>();
					System.err.println(look_up.inscreverBroker("127.0.0.1:" + porta, tipo));
					mensagemDosTopicos(mapaTopicosDoBroker, "127.0.0.1:" + porta, tipo, nome);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}.start();
	}

	public static void createPublisher(String topico, String texto, String porta, String nome) {
		new Thread() {
			public void run() {
				try {
					String brokerIP = look_up.buscarBroker();
					System.err.println(nome + " - broker Externo selecionado: " + brokerIP);
					publicarInscreverNoTopico(topico, texto, brokerIP, "127.0.0.1:" + porta);
					System.err.println(nome + " - Publicacao enviada: " + texto);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}.start();
	}

	public static void createSubscriber(ArrayList<String> topicoList, String porta, String nome) {
		new Thread() {
			public void run() {
				try {
					String brokerIP = look_up.buscarBroker();
					String meuIP = "127.0.0.1:" + porta;

					// Colocar String topico para ser Array<String>, fazer um for das tres linhas de
					// baixo para cada iteracaodo for

					for (String topico : topicoList) {
						publicarInscreverNoTopico(topico, ":STAQ" + meuIP, brokerIP, meuIP);
						System.err.println(nome + " - Inscrito em: " + topico);
					}
					mensagemDosTopicosAssinados(meuIP, nome);

					/*
					 * publicarInscreverNoTopico(topico, ":STAQ"+meuIP, brokerIP, meuIP);
					 * System.err.println(nome + " - Inscrito em: " + topico);
					 * mensagemDosTopicosAssinados(meuIP, nome);
					 */
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}.start();
	}

	///////////////////////////////////
	public static String publicarInscreverNoTopico(String topico, String texto, String brokerIP, String meuIp)
			throws RemoteException {
		// Criar thread que envia o topico e texto para o brokerIP
		try {
			Socket socket = new Socket(brokerIP.substring(0, brokerIP.indexOf(":")),
					Integer.valueOf(brokerIP.substring(brokerIP.indexOf(":") + 1, brokerIP.length())));

			DataOutputStream fluxoSaidaDados = new DataOutputStream(socket.getOutputStream());
			threadEnviarTopicoParaBroker(fluxoSaidaDados, texto, topico, meuIp);

		} catch (IOException iec) {
			System.out.println(iec.getMessage());
		}

		return "Publicado";
	}

	private static void threadEnviarTopicoParaBroker(final DataOutputStream fluxoSaidaDados, final String texto,
			final String topico, final String meuIp) throws IOException {

		new Thread() {
			public void run() {
				try {
					fluxoSaidaDados.writeUTF(topico + "-" + texto + "[" + meuIp);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();
	}

	private static void mensagemDosTopicos(Map<String, ArrayList<String>> mapaTopicosDoBroker, String meuIp,
			String tipo, String nome) {
		new Thread() {
			public void run() {
				try {

					ServerSocket servidorSocket = new ServerSocket(
							Integer.valueOf(meuIp.substring(meuIp.indexOf(":") + 1, meuIp.length())));

					while (true) {
						Socket socket = servidorSocket.accept();
						DataInputStream fluxoEntradaDados = new DataInputStream(socket.getInputStream());
						String mensagem = fluxoEntradaDados.readUTF();

						// Salvar no mapa de topicos o topico
						ArrayList<String> inscritos = new ArrayList<>();

						String topico = mensagem.substring(0, mensagem.indexOf("-"));
						String texto = mensagem.substring(mensagem.indexOf("-") + 1, mensagem.indexOf("["));
						String ultimoIp = mensagem.substring(mensagem.indexOf("[") + 1, mensagem.length());

						// Inscrever no topico
						if (texto.contains(":") && texto.substring(0, texto.indexOf(":") + 5).equals(":STAQ")) {
							if (mapaTopicosDoBroker.containsKey(topico)) {
								inscritos = mapaTopicosDoBroker.get(topico);
								inscritos.add(texto.substring(texto.indexOf(":") + 5, texto.length()));
								mapaTopicosDoBroker.put(topico, inscritos);
							} else {
								inscritos.add(texto.substring(texto.indexOf(":") + 5, texto.length()));
								mapaTopicosDoBroker.put(topico, inscritos);
							}

							System.err.println(
									nome + " - Cliente: " + texto.substring(texto.indexOf(":") + 5, texto.length())
											+ " inscrito no topico: " + topico);
						} else {

							// Usar algoritmo de filtering para repassar aos outros brokers
							ArrayList<String> listaBrokerDoBroker = look_up.listaAdjacencia(meuIp, tipo);
							// System.err.println("lista de brokers: ");
							for (String broker : listaBrokerDoBroker) {
								// System.err.println(broker + " - " + ultimoIp);
								if (broker.contains(":") && !broker.equals(ultimoIp)) {
									publicarInscreverNoTopico(topico, texto,
											broker.substring(broker.indexOf(":"), broker.length()), meuIp);
								}
							}

							// Enviar aos inscritos que assinam o topico
							// publicar(String topico, String texto, String inscritoIP) para cada inscrito
							if (tipo.equals("E")) {
								inscritos = new ArrayList<>();
								if (mapaTopicosDoBroker.containsKey(topico)) {
									inscritos = mapaTopicosDoBroker.get(topico);
									// System.err.println("topico: " + topico);
									// System.err.println("texto:" + texto);
									for (String inscrito : inscritos) {
										publicarInscreverNoTopico(topico, texto,
												inscrito.substring(inscrito.indexOf(":"), inscrito.length()), meuIp);
									}
								}
							}

							// System.err.println("topicos: ");
							for (Map.Entry<String, ArrayList<String>> entry : mapaTopicosDoBroker.entrySet()) {
								// System.err.println(entry.getKey());
							}
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();
	}

	private static void mensagemDosTopicosAssinados(String meuIp, String nome) {
		new Thread() {
			public void run() {
				try {

					ServerSocket servidorSocket = new ServerSocket(
							Integer.valueOf(meuIp.substring(meuIp.indexOf(":") + 1, meuIp.length())));

					while (true) {
						Socket socket = servidorSocket.accept();
						DataInputStream fluxoEntradaDados = new DataInputStream(socket.getInputStream());
						String mensagem = fluxoEntradaDados.readUTF();

						String topico = mensagem.substring(0, mensagem.indexOf("-"));
						String texto = mensagem.substring(mensagem.indexOf("-") + 1, mensagem.indexOf("["));

						System.err.println(nome + " - Topico:" + topico + "\n" + nome + " - Texto:" + texto);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();
	}
}
