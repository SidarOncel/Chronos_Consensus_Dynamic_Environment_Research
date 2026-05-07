#####################################################################
# Chronos Consensus - RAFT Experimental Platform
#
# This Makefile builds the consensus core for:
#   - Milk-V Duo / RISC-V
#   - Linux simulation

#
# Why this matters:
#   Reproducibility is important in research projects.
#   A clean Makefile helps other researchers rebuild your results.
#####################################################################


#####################################################################
# Toolchain selection
#####################################################################

# Default compiler:
# RISC-V Linux cross-compiler
#
# This builds binaries for Milk-V instead of x86 PCs.
#
# Example output:
#   ELF 64-bit LSB executable, UCB RISC-V
#
CC = riscv64-linux-gnu-gcc


#####################################################################
# Build configuration
#####################################################################

# Compiler flags:
#
# -Wall
#   enable most warnings
#
# -Wextra
#   additional useful warnings
#
# -O2
#   moderate optimization
#
# -static
#   produce mostly self-contained binary
#
# -no-pie
#   simpler executable layout
#
# -g
#   include debug symbols
#
# -std=c11
#   modern C standard
#
CFLAGS = -Wall -Wextra -O2 -static -no-pie -g -std=c11


#####################################################################
# Project structure
#####################################################################

# Source files
#
# Keep this list explicit for readability.
#
SRC = \
    main.c \
    raft.c \
    network.c \
    hal.c

# Assembly sources
ASM_SRC = \
    raft_core.S

# Object files generated from C
OBJ = $(SRC:.c=.o)

# Object files generated from assembly
ASM_OBJ = $(ASM_SRC:.S=.o)

# Final executable
TARGET = chronos_raft


#####################################################################
# Default target
#####################################################################

# Running "make" alone builds the target.
#
all: $(TARGET)


#####################################################################
# Link final executable
#####################################################################

# Link:
#   C objects
#   assembly objects
#
# into one final executable.
#
$(TARGET): $(OBJ) $(ASM_OBJ)
	$(CC) $(CFLAGS) -o $@ $^

	@echo ""
	@echo "======================================="
	@echo " Build completed successfully"
	@echo " Target: $(TARGET)"
	@echo "======================================="
	@echo ""


#####################################################################
# Compile C source files
#####################################################################

# Example:
#   main.c -> main.o
#
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@


#####################################################################
# Compile assembly source files
#####################################################################

# Example:
#   raft_core.S -> raft_core.o
#
%.o: %.S
	$(CC) $(CFLAGS) -c $< -o $@


#####################################################################
# Run locally (Linux simulation)
#####################################################################

# This target is mainly useful when:
#   - testing on Linux
#   - running localhost experiments
#   - debugging before copying to Milk-V
#
run: $(TARGET)
	./$(TARGET)


#####################################################################
# Copy executable to Milk-V
#####################################################################

# Adjust the IP address if needed.
#
# Example usage:
#   make upload
#
# Requires:
#   - ssh/scp access
#   - Milk-V connected over USB Ethernet or LAN
#
MILKV_IP ?= 192.168.42.1

upload: $(TARGET)
	scp $(TARGET) root@$(MILKV_IP):/root/


#####################################################################
# Run directly on Milk-V
#####################################################################

# Example:
#   make remote-run
#
remote-run: upload
	ssh root@$(MILKV_IP) "./$(TARGET)"


#####################################################################
# Clean build artifacts
#####################################################################

# Removes:
#   - object files
#   - executable
#
clean:
	rm -f $(OBJ)
	rm -f $(ASM_OBJ)
	rm -f $(TARGET)

	@echo "Build artifacts removed."


#####################################################################
# Help target
#####################################################################

help:
	@echo ""
	@echo "Chronos Consensus Build System"
	@echo ""
	@echo "Targets:"
	@echo "  make            Build project"
	@echo "  make run        Run locally"
	@echo "  make upload     Copy binary to Milk-V"
	@echo "  make remote-run Upload and run on Milk-V"
	@echo "  make clean      Remove build files"
	@echo ""