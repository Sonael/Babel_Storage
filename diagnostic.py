#!/usr/bin/env python3
"""
Babel Upload Diagnostic Tool

Use this script to diagnose issues with uploading PDFs to the Library of Babel.
"""

import os
import sys
import hashlib
import binary_encoder
import file_chunker
import babel


def diagnose_pdf(pdf_path: str):
    """Diagnostica problemas com o upload de PDF."""
    
    print("=" * 70)
    print("BABEL PDF UPLOAD DIAGNOSTIC")
    print("=" * 70)
    
    # 1. Verifica se o arquivo existe
    if not os.path.exists(pdf_path):
        print(f"❌ Arquivo não encontrado: {pdf_path}")
        return
    
    print(f"\n1️⃣ Arquivo encontrado: {pdf_path}")
    file_size = os.path.getsize(pdf_path)
    print(f"   Tamanho: {file_size:,} bytes ({file_size/1024:.2f} KB)")
    
    # 2. Testa conexão com Babel
    print("\n2️⃣ Testando conexão com Library of Babel...")
    try:
        if hasattr(babel, 'test_connection'):
            connected = babel.test_connection(verbose=False)
        else:
            # Fallback: tenta uma busca simples
            print("   Tentando busca de teste...")
            result = babel.search("test")
            connected = result[0] is not None
            
        if connected:
            print("   ✅ Conexão OK")
        else:
            print("   ⚠️ Conexão instável ou sem resposta")
    except Exception as e:
        print(f"   ❌ Erro de conexão: {e}")
        print("\n   💡 POSSÍVEIS SOLUÇÕES:")
        print("   - Verifique sua conexão com a internet")
        print("   - O site libraryofbabel.info pode estar temporariamente fora do ar")
        print("   - Tente novamente em alguns minutos")
        return
    
    # 3. Cria metadata do arquivo
    print("\n3️⃣ Criando metadata do arquivo...")
    try:
        metadata = file_chunker.create_file_metadata(pdf_path)
        print(f"   ✅ Metadata criado")
        print(f"   Chunks: {metadata.chunk_count}")
        print(f"   SHA256: {metadata.file_hash[:16]}...")
        
        # Estimativa de tempo
        est_time = metadata.chunk_count * 3  # 1.5s por chunk
        print(f"   Tempo estimado: ~{est_time:.0f}s ({est_time/60:.1f} min)")
        
    except Exception as e:
        print(f"   ❌ Erro ao criar metadata: {e}")
        return
    
    # 4. Testa o primeiro chunk
    print("\n4️⃣ Testando o primeiro chunk...")
    try:
        chunk_generator = file_chunker.split_file_into_chunks(pdf_path)
        chunk_index, chunk_data = next(chunk_generator)
        
        print(f"   Chunk index: {chunk_index}")
        print(f"   Chunk size: {len(chunk_data)} bytes")
        print(f"   Chunk SHA256: {hashlib.sha256(chunk_data).hexdigest()[:16]}...")
        
        # 5. Testa encoding
        print("\n5️⃣ Testando encoding do chunk...")
        encoded = binary_encoder.encode_bytes_to_babel(chunk_data)
        print(f"   ✅ Encoded com sucesso")
        print(f"   Tamanho encoded: {len(encoded)} caracteres")
        print(f"   Preview: {encoded[:50]}...")
        
        # Verifica se está dentro do limite
        if len(encoded) > 3200:
            print(f"   ⚠️ WARNING: Texto muito longo! ({len(encoded)} > 3200)")
            print("   O Babel pode rejeitar textos muito longos")
            print("\n   💡 SOLUÇÃO: Reduza o tamanho dos chunks")
            print("   Edite file_chunker.py e reduza MAX_CHUNK_BYTES")
            return
        else:
            print(f"   ✅ Tamanho OK (limite: 3200)")
        
        # Diagnostica o texto encoded
        if hasattr(babel, 'diagnose_search_failure'):
            print("\n6️⃣ Diagnóstico detalhado do texto...")
            issues = babel.diagnose_search_failure(encoded)
            
            if issues['too_long']:
                print(f"   ❌ Texto muito longo: {issues['text_length']} caracteres")
            elif issues['invalid_chars']:
                print(f"   ❌ Caracteres inválidos: {issues['invalid_chars']}")
            elif issues['is_empty']:
                print(f"   ❌ Texto vazio!")
            else:
                print("   ✅ Texto válido")
        
        # 6. Testa busca no Babel
        print("\n7️⃣ Testando busca no Babel (pode demorar ~30s)...")
        try:
            # Usa verbose se disponível
            if 'verbose' in babel.search.__code__.co_varnames:
                result = babel.search(encoded, verbose=True)
            else:
                result = babel.search(encoded)
            
            hex_id, wall, shelf, volume, page = result
            
            if not hex_id:
                print("   ❌ Babel retornou coordenadas vazias!")
                print("\n   💡 POSSÍVEIS CAUSAS:")
                print("   1. O texto é muito longo (> 3200 chars)")
                print("   2. Rate limiting do servidor Babel")
                print("   3. Timeout do servidor")
                print("   4. Mudança no HTML da página")
                print("\n   💡 SOLUÇÕES:")
                print("   - Aguarde alguns minutos e tente novamente")
                print("   - Reduza o tamanho dos chunks")
                print("   - Verifique se o site está funcionando: https://libraryofbabel.info")
            else:
                print("   ✅ Coordenadas encontradas!")
                print(f"   Hexagon: {hex_id[:20]}...")
                print(f"   Wall: {wall}, Shelf: {shelf}")
                print(f"   Volume: {volume}, Page: {page}")
                
                # 7. Verifica recuperação
                print("\n8️⃣ Verificando recuperação dos dados...")
                try:
                    retrieved = babel.browse(hex_id, wall, shelf, volume, page)
                    if retrieved:
                        retrieved_clean = retrieved.replace("\n", "").replace("\r", "")
                        
                        if retrieved_clean[:len(encoded)] == encoded:
                            print("   ✅ Dados verificados com sucesso!")
                            print("\n" + "=" * 70)
                            print("✅ DIAGNÓSTICO COMPLETO: Sistema funcionando corretamente!")
                            print("=" * 70)
                        else:
                            print("   ⚠️ Dados recuperados não correspondem ao original")
                    else:
                        print("   ❌ Não foi possível recuperar os dados")
                except Exception as e:
                    print(f"   ❌ Erro ao recuperar: {e}")
                    
        except Exception as e:
            print(f"   ❌ Erro na busca: {e}")
            print(f"\n   Detalhes do erro:")
            import traceback
            traceback.print_exc()
            
    except StopIteration:
        print("   ❌ Arquivo vazio ou sem chunks")
    except Exception as e:
        print(f"   ❌ Erro: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 diagnostic.py <caminho_do_pdf>")
        print("\nExemplo:")
        print("  python3 diagnostic.py meu_arquivo.pdf")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    diagnose_pdf(pdf_path)