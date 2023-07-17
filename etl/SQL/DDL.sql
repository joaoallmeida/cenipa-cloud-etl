CREATE DATABASE IF NOT EXISTS `dw` /*!40100 DEFAULT CHARACTER SET latin1 */;


DROP TABLE IF EXISTS dim_aeronave;
DROP TABLE IF EXISTS dim_ocorrencia;
DROP TABLE IF EXISTS dim_ocorrencia_tipo;
DROP TABLE IF EXISTS dim_recomendacao;
DROP TABLE IF EXISTS fat_ocorrencia;

-- dw.fat_ocorrencia definition

CREATE TABLE IF NOT EXISTS `dw`.`fat_ocorrencia` (
  `codigo_ocorrencia` bigint(20) DEFAULT NULL,
  `codigo_ocorrencia_tipo` bigint(20) DEFAULT NULL,
  `codigo_ocorrencia_aeronave` bigint(20) DEFAULT NULL,
  `codigo_ocorrencia_recomendacao` bigint(20) DEFAULT NULL,
  `ocorrencia_classificacao` varchar(255),
  `ocorrencia_latitude` varchar(255),
  `ocorrencia_longitude` varchar(255),
  `ocorrencia_cidade` varchar(255),
  `ocorrencia_uf` varchar(255),
  `ocorrencia_pais` varchar(255),
  `ocorrencia_aerodromo` varchar(255),
  `ocorrencia_dia` datetime DEFAULT NULL,
  `ocorrencia_hora` varchar(255),
  `investigacao_aeronave_liberada` varchar(255),
  `investigacao_status` varchar(255),
  `divulgacao_relatorio_numero` varchar(255),
  `divulgacao_relatorio_publicado` varchar(255),
  `divulgacao_dia_publicacao` datetime DEFAULT NULL,
  `total_recomendacoes` bigint(20) DEFAULT NULL,
  `total_aeronaves_envolvidas` bigint(20) DEFAULT NULL,
  `ocorrencia_saida_pista` varchar(255),
  `criado_em` datetime DEFAULT NULL,
  `criado_por` varchar(255),
  `atualizado_em` datetime DEFAULT NULL,
  `atualizado_por` varchar(255)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- dw.dim_recomendacao definition

CREATE TABLE IF NOT EXISTS `dw`.`dim_recomendacao` (
  `codigo_ocorrencia` bigint(20) DEFAULT NULL,
  `recomendacao_numero` varchar(255),
  `recomendacao_dia_assinatura` varchar(255) DEFAULT NULL,
  `recomendacao_dia_encaminhamento` varchar(255) DEFAULT NULL,
  `recomendacao_dia_feedback` varchar(255) DEFAULT NULL,
  `recomendacao_conteudo` varchar(10000),
  `recomendacao_status` varchar(255),
  `recomendacao_destinatario_sigla` varchar(255),
  `recomendacao_destinatario` varchar(255),
  `criado_em` datetime DEFAULT NULL,
  `criado_por` varchar(255),
  `atualizado_em` datetime DEFAULT NULL,
  `atualizado_por` varchar(255)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- dw.dim_ocorrencia_tipo definition

CREATE TABLE IF NOT EXISTS `dw`.`dim_ocorrencia_tipo` (
  `codigo_ocorrencia` bigint(20) DEFAULT NULL,
  `ocorrencia_tipo` varchar(255),
  `ocorrencia_tipo_categoria` varchar(255),
  `taxonomia_tipo_icao` varchar(255),
  `criado_em` datetime DEFAULT NULL,
  `criado_por` varchar(255),
  `atualizado_em` datetime DEFAULT NULL,
  `atualizado_por` varchar(255)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- dw.dim_ocorrencia definition

CREATE TABLE IF NOT EXISTS `dw`.`dim_ocorrencia` (
  `codigo_ocorrencia` bigint(20) DEFAULT NULL,
  `ocorrencia_tipo` varchar(255),
  `ocorrencia_tipo_categoria` varchar(255),
  `taxonomia_tipo_icao` varchar(255),
  `criado_em` datetime DEFAULT NULL,
  `criado_por` varchar(255),
  `atualizado_em` datetime DEFAULT NULL,
  `atualizado_por` varchar(255)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- dw.dim_aeronave definition

CREATE TABLE IF NOT EXISTS `dw`.`dim_aeronave` (
  `codigo_ocorrencia` bigint(20) DEFAULT NULL,
  `aeronave_matricula` varchar(255),
  `aeronave_operador_categoria` varchar(255),
  `aeronave_tipo_veiculo` varchar(255),
  `aeronave_fabricante` varchar(255),
  `aeronave_modelo` varchar(255),
  `aeronave_tipo_icao` varchar(255),
  `aeronave_motor_tipo` varchar(255),
  `aeronave_motor_quantidade` varchar(255),
  `aeronave_pmd` double DEFAULT NULL,
  `aeronave_pmd_categoria` double DEFAULT NULL,
  `aeronave_assentos` double DEFAULT NULL,
  `aeronave_ano_fabricacao` double DEFAULT NULL,
  `aeronave_pais_fabricante` varchar(255),
  `aeronave_pais_registro` varchar(255),
  `aeronave_registro_categoria` varchar(255),
  `aeronave_registro_segmento` varchar(255),
  `aeronave_voo_origem` varchar(255),
  `aeronave_voo_destino` varchar(255),
  `aeronave_fase_operacao` varchar(255),
  `aeronave_tipo_operacao` varchar(255),
  `aeronave_nivel_dano` varchar(255),
  `aeronave_fatalidades_total` double DEFAULT NULL,
  `criado_em` datetime DEFAULT NULL,
  `criado_por` varchar(255),
  `atualizado_em` datetime DEFAULT NULL,
  `atualizado_por` varchar(255)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;