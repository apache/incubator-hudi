/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
grammar HoodieSqlCommon;

 singleStatement
    : statement EOF
    ;

statement
    : compactionStatement                                              #compactionCommand
    | .*?                                                              #passThrough
    ;

 compactionStatement
    : operation = (RUN | SCHEDULE) COMPACTION  ON tableIdentifier (AT timestamp = NUMBER)?    #compactionOnTable
    | operation = (RUN | SCHEDULE) COMPACTION  ON path = STRING   (AT timestamp = NUMBER)?    #compactionOnPath
    | SHOW COMPACTION  ON tableIdentifier (LIMIT limit = NUMBER)?                             #showCompactionOnTable
    | SHOW COMPACTION  ON path = STRING (LIMIT limit = NUMBER)?                               #showCompactionOnPath
    ;

 tableIdentifier
    : (db=IDENTIFIER '.')? table=IDENTIFIER
    ;

 ALL: 'ALL';
 AT: 'AT';
 COMPACTION: 'COMPACTION';
 RUN: 'RUN';
 SCHEDULE: 'SCHEDULE';
 ON: 'ON';
 SHOW: 'SHOW';
 LIMIT: 'LIMIT';

 NUMBER
    : DIGIT+
    ;

 IDENTIFIER
     : (LETTER | DIGIT | '_')+
     ;

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;



 fragment DIGIT
     : [0-9]
     ;

 fragment LETTER
     : [A-Z]
     ;

 SIMPLE_COMMENT
     : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
     ;

 BRACKETED_COMMENT
     : '/*' .*? '*/' -> channel(HIDDEN)
     ;

 WS  : [ \r\n\t]+ -> channel(HIDDEN)
     ;

 // Catch-all for anything we can't recognize.
 // We use this to be able to ignore and recover all the text
 // when splitting statements with DelimiterLexer
 UNRECOGNIZED
     : .
     ;
