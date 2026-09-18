# 01 Mini Desafio Técnico Cumbuca

## Descrição Conceitual

Suponha que temos 2 livros-razão (ledgers).

Um dos livros-razão (externo) armazena transações Pix de clientes.
Isto é: Toda vez que um cliente quer fazer um Pix ou que nós recebemos dinheiro de fora do sistema, uma ou mais novas transações são geradas.

Transações no livro-razão externo são recebidas por uma interface HTTP desenvolvida no padrão REST ou por uma fila de eventos
(para o caso de recebimentos Pix recebidos de outras instituições).

O segundo livro-razão (interno) armazena transações de nossas contas operacionais internas.
Deve ser considerado que existem as seguintes contas operacionais:

- Reserva Financeira -> Esta conta armazena recursos da própria instituição.
- Recursos de Clientes -> Esta conta armazena os recursos dos clientes, sem divisão por propriedade deles.
- Conta de Pagamentos Instantâneos -> Valores a transacionar no Pix só podem sair desta conta.

As transações no livro-razão externo serão geradas por serviços internos ou interfaces administrativas internas.

## O Desafio

Seu objetivo é propor uma arquitetura que atenda aos seguintes requisitos:

1. Nenhum cliente nunca deveria ver falhas no Pix. Isto é, a Conta de Pagamentos Instantâneos nunca deveria ter menos recursos do que o valor que um cliente possa legitimamente querer transacionar.
2. Todo dia, às 18h (BRT) a conta Recursos de Clientes deve ter valor equivalente ao total de todos os saldos de clientes.
3. Os ledgers devem ser eventualmente consistentes. Isto é, no menor tempo possível após uma operação ser efetuada, contas que dependam daquela operação devem ser atualizadas. Invariantes que devem ser mantidos antes e após a consistência ser reestabelecida:

- Total dos saldos do livro-razão "transações de clientes" = Saldo Recursos de Clientes + Saldo Conta de Pagamentos Instantâneos;
- Nunca deve ser possível "criar" nem destruir dinheiro;
- Os livros-razão devem seguir contabilidade de dupla entrada.

Para tal, será necessário criar uma política de reabastecimento das contas internas que garanta da melhor forma possível as propriedades listadas.

Além disso, será necessário especificar as operações internas e como ocorrerão (cronjobs? dashboards internos? etc)

## O que você deve entregar

Um documento .md, tal como este, explicando as tecnologias a serem adotadas, interfaces a serem expostas e dando uma breve motivação para cada escolha.
